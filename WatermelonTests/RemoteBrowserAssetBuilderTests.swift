import XCTest
@testable import Watermelon

// The remote/merged browser shows MEANINGFUL records (complete or partial-but-has-media), flagged when
// incomplete so the user decides at download time. The meaningless ones are dropped: a phantom (no resolvable
// resource) or a config-only record (only an adjustment sidecar) has no photo/video to show and isn't a real
// backup — the future "incomplete resources" entry will own those.
final class RemoteBrowserAssetBuilderTests: XCTestCase {
    private let year = 2024
    private let month = 3
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: year, month: month) }

    private func resource(_ name: String, _ hash: Data, role: Int) -> RemoteManifestResource {
        RemoteManifestResource(year: year, month: month, fileName: name, contentHash: hash, fileSize: 100, resourceType: role, creationDateMs: 0, backedUpAtMs: 0)
    }
    private func link(_ fp: Data, _ hash: Data, role: Int, slot: Int = 0) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fp, resourceHash: hash, role: role, slot: slot)
    }
    private func asset(_ fp: Data, count: Int, creationDateMs: Int64? = 0, backedUpAtMs: Int64 = 0) -> RemoteManifestAsset {
        RemoteManifestAsset(year: year, month: month, assetFingerprint: fp, creationDateMs: creationDateMs, backedUpAtMs: backedUpAtMs, resourceCount: count, totalFileSizeBytes: 100)
    }
    private func fingerprint(of links: [RemoteAssetResourceLink]) -> Data {
        BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: links.map { (role: $0.role, slot: $0.slot, contentHash: $0.resourceHash) })
    }

    func testHasBackedUpMedia() {
        let hPhoto = Data([1]); let hMeta = Data([7]); let hMissing = Data([9])
        let available: Set<Data> = [hPhoto, hMeta]
        let isAvail: (Data) -> Bool = { available.contains($0) }
        // Photo present → backed up.
        XCTAssertTrue(MonthManifestStore.hasBackedUpMedia(links: [link(Data(), hPhoto, role: 1)], isResourceAvailable: isAvail))
        // Photo present + a missing paired video (role 5) → still backed up (it has real media).
        XCTAssertTrue(MonthManifestStore.hasBackedUpMedia(links: [link(Data(), hPhoto, role: 1), link(Data(), hMissing, role: 5)], isResourceAvailable: isAvail))
        // Only a config-only adjustment sidecar (role 7) → not backed up.
        XCTAssertFalse(MonthManifestStore.hasBackedUpMedia(links: [link(Data(), hMeta, role: 7)], isResourceAvailable: isAvail))
        // A media role whose resource is absent → not backed up.
        XCTAssertFalse(MonthManifestStore.hasBackedUpMedia(links: [link(Data(), hMissing, role: 1)], isResourceAvailable: isAvail))
        // No links → not backed up.
        XCTAssertFalse(MonthManifestStore.hasBackedUpMedia(links: [], isResourceAvailable: isAvail))
    }

    func testBuilderFlagsIncompleteAndDropsUndisplayable() {
        let hPhoto = Data([1]); let hPhotoB = Data([2]); let hMissing = Data([9]); let hMeta = Data([7])

        // Complete photo (role 1) — shown, not incomplete.
        let completeLinks = [link(Data(), hPhoto, role: 1)]
        let fpComplete = fingerprint(of: completeLinks)
        let complete = completeLinks.map { link(fpComplete, $0.resourceHash, role: $0.role) }

        // Partial: role 1 present + role 5 (fullSizePhoto) missing → incomplete but has a resolvable link → shown, flagged.
        let partialLinks = [link(Data(), hPhotoB, role: 1), link(Data(), hMissing, role: 5)]
        let fpPartial = fingerprint(of: partialLinks)
        let partial = partialLinks.map { link(fpPartial, $0.resourceHash, role: $0.role) }

        // Metadata-only (role 7 == adjustmentData) present → no real media → meaningless → dropped.
        let metaLinks = [link(Data(), hMeta, role: 7)]
        let fpMeta = fingerprint(of: metaLinks)
        let meta = metaLinks.map { link(fpMeta, $0.resourceHash, role: $0.role) }

        // Fully unresolvable (its only resource is absent) → nothing to display → dropped.
        let fpGhost = Data([0xEE])
        let ghost = [link(fpGhost, Data([0xEF]), role: 1)]

        let delta = RemoteLibraryMonthDelta(
            month: monthKey,
            resources: [resource("a.jpg", hPhoto, role: 1), resource("b.jpg", hPhotoB, role: 1), resource("m.json", hMeta, role: 7)],
            assets: [asset(fpComplete, count: 1), asset(fpPartial, count: 2), asset(fpMeta, count: 1), asset(fpGhost, count: 1)],
            assetResourceLinks: complete + partial + meta + ghost
        )
        let state = RemoteLibrarySnapshotState(revision: 1, isFullSnapshot: true, monthDeltas: [delta], profileKey: "p")

        let built = RemoteBrowserAssetBuilder.build(from: state)
        let byFp = Dictionary(uniqueKeysWithValues: (built.assetsByMonth[monthKey] ?? []).map { ($0.fingerprint, $0.isIncomplete) })

        XCTAssertEqual(byFp[fpComplete], false, "complete asset shown, not flagged")
        XCTAssertEqual(byFp[fpPartial], true, "partial-but-has-media asset shown, flagged incomplete")
        XCTAssertNil(byFp[fpMeta], "config-only (metadata) asset is dropped (no real media, not a backup)")
        XCTAssertNil(byFp[fpGhost], "fully-unresolvable asset is dropped (nothing to display)")

        var metrics: RemoteBrowserProjectionMetrics?
        let projection = RemoteBrowserAssetBuilder.buildProjection(
            from: state,
            includeBrowserAssets: true,
            collectPresence: true,
            onMetrics: { metrics = $0 }
        )
        XCTAssertEqual(projection?.remoteFingerprints, Set([fpComplete, fpPartial, fpMeta, fpGhost]))
        XCTAssertEqual(projection?.backedUpFingerprints, Set([fpComplete, fpPartial]))
        XCTAssertEqual(projection?.completeFingerprints, Set([fpComplete]))
        XCTAssertEqual(projection?.assetsByMonth, built.assetsByMonth)
        XCTAssertEqual(projection?.ownerProfileKey, "p")
        XCTAssertEqual(projection?.attachingDeviceHandles([fpComplete: "local"]).ownerProfileKey, "p")
        XCTAssertEqual(metrics?.resourceCount, 3)
        XCTAssertEqual(metrics?.linkCount, 5)
        XCTAssertGreaterThanOrEqual(metrics?.resourceMapMs ?? -1, 0)
        XCTAssertGreaterThanOrEqual(metrics?.linkGroupMs ?? -1, 0)
        XCTAssertGreaterThanOrEqual(metrics?.assetProjectMs ?? -1, 0)
        XCTAssertGreaterThanOrEqual(metrics?.sortMs ?? -1, 0)
        XCTAssertEqual((metrics?.sortPerformedMonths ?? 0) + (metrics?.sortSkippedMonths ?? 0), 1)
    }

    func testConcurrentProjectionMatchesSerialProjection() async throws {
        func delta(month: Int, seed: UInt8) -> RemoteLibraryMonthDelta {
            let monthKey = LibraryMonthKey(year: year, month: month)
            let hash = Data([seed])
            let prototype = RemoteAssetResourceLink(
                year: year,
                month: month,
                assetFingerprint: Data(),
                resourceHash: hash,
                role: ResourceTypeCode.photo,
                slot: 0
            )
            let fingerprint = self.fingerprint(of: [prototype])
            return RemoteLibraryMonthDelta(
                month: monthKey,
                resources: [
                    RemoteManifestResource(
                        year: year,
                        month: month,
                        fileName: "\(seed).jpg",
                        contentHash: hash,
                        fileSize: 100,
                        resourceType: ResourceTypeCode.photo,
                        creationDateMs: 0,
                        backedUpAtMs: 0
                    ),
                ],
                assets: [
                    RemoteManifestAsset(
                        year: year,
                        month: month,
                        assetFingerprint: fingerprint,
                        creationDateMs: Int64(seed),
                        backedUpAtMs: 0,
                        resourceCount: 1,
                        totalFileSizeBytes: 100
                    ),
                ],
                assetResourceLinks: [
                    RemoteAssetResourceLink(
                        year: year,
                        month: month,
                        assetFingerprint: fingerprint,
                        resourceHash: hash,
                        role: ResourceTypeCode.photo,
                        slot: 0
                    ),
                ]
            )
        }
        let state = RemoteLibrarySnapshotState(
            revision: 7,
            isFullSnapshot: true,
            monthDeltas: [
                delta(month: 1, seed: 1),
                delta(month: 2, seed: 2),
                delta(month: 3, seed: 3),
                delta(month: 4, seed: 4),
            ],
            profileKey: "p"
        )
        let serial = try XCTUnwrap(RemoteBrowserAssetBuilder.buildProjection(
            from: state,
            includeBrowserAssets: true,
            collectPresence: true
        ))
        var metrics: RemoteBrowserProjectionMetrics?
        let concurrentResult = await RemoteBrowserAssetBuilder.buildProjectionConcurrently(
            from: state,
            includeBrowserAssets: true,
            collectPresence: true,
            maximumWorkerCount: 3,
            onMetrics: { metrics = $0 }
        )
        let concurrent = try XCTUnwrap(concurrentResult)

        XCTAssertEqual(concurrent.revision, serial.revision)
        XCTAssertEqual(concurrent.ownerProfileKey, serial.ownerProfileKey)
        XCTAssertEqual(concurrent.months, serial.months)
        XCTAssertEqual(concurrent.assetsByMonth, serial.assetsByMonth)
        XCTAssertEqual(concurrent.remoteFingerprints, serial.remoteFingerprints)
        XCTAssertEqual(concurrent.backedUpFingerprints, serial.backedUpFingerprints)
        XCTAssertEqual(concurrent.completeFingerprints, serial.completeFingerprints)
        XCTAssertEqual(metrics?.workerCount, 3)
        XCTAssertEqual(metrics?.resourceCount, 4)
        XCTAssertEqual(metrics?.linkCount, 4)
    }

    func testBuilderPreservesResourcePriorityAndSlotZeroSelection() throws {
        let alternateHash = Data([4])
        let nonzeroPhotoHash = Data([1])
        let primaryPhotoHash = Data([2])
        let pairedVideoHash = Data([9])
        let prototypes = [
            link(Data(), alternateHash, role: ResourceTypeCode.alternatePhoto),
            link(Data(), nonzeroPhotoHash, role: ResourceTypeCode.photo, slot: 2),
            link(Data(), pairedVideoHash, role: ResourceTypeCode.pairedVideo),
            link(Data(), primaryPhotoHash, role: ResourceTypeCode.photo),
        ]
        let fingerprint = self.fingerprint(of: prototypes)
        let links = prototypes.map {
            link(fingerprint, $0.resourceHash, role: $0.role, slot: $0.slot)
        }
        let delta = RemoteLibraryMonthDelta(
            month: monthKey,
            resources: [
                resource("alternate.jpg", alternateHash, role: ResourceTypeCode.alternatePhoto),
                resource("nonzero.jpg", nonzeroPhotoHash, role: ResourceTypeCode.photo),
                resource("primary.jpg", primaryPhotoHash, role: ResourceTypeCode.photo),
                resource("paired.mov", pairedVideoHash, role: ResourceTypeCode.pairedVideo),
            ],
            assets: [asset(fingerprint, count: links.count)],
            assetResourceLinks: links
        )

        let built = RemoteBrowserAssetBuilder.build(from: RemoteLibrarySnapshotState(
            revision: 1,
            isFullSnapshot: true,
            monthDeltas: [delta],
            profileKey: "p"
        ))
        let item = try XCTUnwrap(built.assetsByMonth[monthKey]?.first)

        XCTAssertTrue(item.isLivePhoto)
        XCTAssertFalse(item.isVideo)
        XCTAssertEqual(item.photoRemoteRelativePath, "2024/03/primary.jpg")
        XCTAssertEqual(item.videoRemoteRelativePath, "2024/03/paired.mov")
        XCTAssertFalse(item.isIncomplete)
    }

    func testBuilderSkipsSortOnlyForStrictlyDescendingItems() {
        func makeState(_ creationDates: [Int64]) -> RemoteLibrarySnapshotState {
            var resources: [RemoteManifestResource] = []
            var assets: [RemoteManifestAsset] = []
            var links: [RemoteAssetResourceLink] = []
            for (index, creationDate) in creationDates.enumerated() {
                let hash = Data([UInt8(index + 1)])
                let prototype = [link(Data(), hash, role: ResourceTypeCode.photo)]
                let fingerprint = self.fingerprint(of: prototype)
                resources.append(resource("\(index).jpg", hash, role: ResourceTypeCode.photo))
                assets.append(asset(fingerprint, count: 1, creationDateMs: creationDate))
                links.append(link(fingerprint, hash, role: ResourceTypeCode.photo))
            }
            return RemoteLibrarySnapshotState(
                revision: 1,
                isFullSnapshot: true,
                monthDeltas: [
                    RemoteLibraryMonthDelta(
                        month: monthKey,
                        resources: resources,
                        assets: assets,
                        assetResourceLinks: links
                    )
                ],
                profileKey: "p"
            )
        }

        var descendingMetrics: RemoteBrowserProjectionMetrics?
        let descending = RemoteBrowserAssetBuilder.buildProjection(
            from: makeState([3_000, 2_000, 1_000]),
            includeBrowserAssets: true,
            collectPresence: false,
            onMetrics: { descendingMetrics = $0 }
        )
        XCTAssertEqual(descending?.assetsByMonth[monthKey]?.map(\.creationDateMs), [3_000, 2_000, 1_000])
        XCTAssertEqual(descendingMetrics?.sortPerformedMonths, 0)
        XCTAssertEqual(descendingMetrics?.sortSkippedMonths, 1)

        var unorderedMetrics: RemoteBrowserProjectionMetrics?
        let unordered = RemoteBrowserAssetBuilder.buildProjection(
            from: makeState([1_000, 3_000, 2_000]),
            includeBrowserAssets: true,
            collectPresence: false,
            onMetrics: { unorderedMetrics = $0 }
        )
        XCTAssertEqual(unordered?.assetsByMonth[monthKey]?.map(\.creationDateMs), [3_000, 2_000, 1_000])
        XCTAssertEqual(unorderedMetrics?.sortPerformedMonths, 1)
        XCTAssertEqual(unorderedMetrics?.sortSkippedMonths, 0)
    }

    func testBuilderUsesStableFingerprintOrderForEqualCreationDates() {
        func makeState(reversed: Bool) -> RemoteLibrarySnapshotState {
            let hashes = [Data([0x01]), Data([0x02]), Data([0x03])]
            let fingerprints = hashes.map { hash in
                fingerprint(of: [link(Data(), hash, role: ResourceTypeCode.photo)])
            }
            let order = reversed ? Array(fingerprints.indices.reversed()) : Array(fingerprints.indices)
            return RemoteLibrarySnapshotState(
                revision: 1,
                isFullSnapshot: true,
                monthDeltas: [
                    RemoteLibraryMonthDelta(
                        month: monthKey,
                        resources: order.map {
                            resource("\($0).jpg", hashes[$0], role: ResourceTypeCode.photo)
                        },
                        assets: order.map {
                            asset(fingerprints[$0], count: 1, creationDateMs: 1_000)
                        },
                        assetResourceLinks: order.map {
                            link(fingerprints[$0], hashes[$0], role: ResourceTypeCode.photo)
                        }
                    ),
                ],
                profileKey: "p"
            )
        }
        let expected = RemoteBrowserAssetBuilder.build(from: makeState(reversed: false))
            .assetsByMonth[monthKey]?
            .map(\.fingerprint)
        let reversed = RemoteBrowserAssetBuilder.build(from: makeState(reversed: true))
            .assetsByMonth[monthKey]?
            .map(\.fingerprint)

        XCTAssertEqual(reversed, expected)
        XCTAssertEqual(
            expected,
            expected?.sorted { $0.lexicographicallyPrecedes($1) }
        )
    }

    func testBuilderUsesCanonicalEpochForMissingOrInvalidCreationDate() {
        let hash = Data([1])
        let missingFP = Data([2])
        let invalidFP = Data([3])
        let delta = RemoteLibraryMonthDelta(
            month: monthKey,
            resources: [resource("a.jpg", hash, role: 1)],
            assets: [
                asset(missingFP, count: 1, creationDateMs: nil, backedUpAtMs: 1_700_000_000_000),
                asset(invalidFP, count: 1, creationDateMs: .max, backedUpAtMs: 1_700_000_000_000)
            ],
            assetResourceLinks: [
                link(missingFP, hash, role: 1),
                link(invalidFP, hash, role: 1)
            ]
        )

        let built = RemoteBrowserAssetBuilder.build(from: RemoteLibrarySnapshotState(
            revision: 1,
            isFullSnapshot: true,
            monthDeltas: [delta],
            profileKey: "p"
        ))
        let creationDates = built.assetsByMonth[monthKey]?.map(\.creationDateMs)

        XCTAssertEqual(creationDates, [0, 0])
    }

    func testBuilderStopsWhenCancelled() {
        let hash = Data([1])
        let fingerprint = Data([2])
        let delta = RemoteLibraryMonthDelta(
            month: monthKey,
            resources: [resource("a.jpg", hash, role: 1)],
            assets: [asset(fingerprint, count: 1)],
            assetResourceLinks: [link(fingerprint, hash, role: 1)]
        )
        var checks = 0

        let built = RemoteBrowserAssetBuilder.build(
            from: RemoteLibrarySnapshotState(
                revision: 1,
                isFullSnapshot: true,
                monthDeltas: [delta],
                profileKey: "p"
            ),
            shouldCancel: {
                checks += 1
                return checks >= 2
            }
        )

        XCTAssertTrue(built.months.isEmpty)
        XCTAssertTrue(built.assetsByMonth.isEmpty)
    }

    func testPresenceProjectionTreatsAnyCompleteTwinAsComplete() {
        let hash = Data([4])
        let prototype = [link(Data(), hash, role: 1)]
        let fingerprint = self.fingerprint(of: prototype)
        let links = [link(fingerprint, hash, role: 1)]
        let complete = RemoteLibraryMonthDelta(
            month: monthKey,
            resources: [resource("a.jpg", hash, role: 1)],
            assets: [asset(fingerprint, count: 1)],
            assetResourceLinks: links
        )
        let incomplete = RemoteLibraryMonthDelta(
            month: LibraryMonthKey(year: year, month: month - 1),
            resources: [],
            assets: [asset(fingerprint, count: 1)],
            assetResourceLinks: links
        )

        let projection = RemoteBrowserAssetBuilder.buildProjection(
            from: RemoteLibrarySnapshotState(
                revision: 2,
                isFullSnapshot: true,
                monthDeltas: [incomplete, complete],
                profileKey: "p"
            ),
            includeBrowserAssets: false,
            collectPresence: true
        )

        XCTAssertEqual(projection?.remoteFingerprints, Set([fingerprint]))
        XCTAssertEqual(projection?.backedUpFingerprints, Set([fingerprint]))
        XCTAssertEqual(projection?.completeFingerprints, Set([fingerprint]))
        XCTAssertTrue(projection?.assetsByMonth.isEmpty == true)
    }
}
