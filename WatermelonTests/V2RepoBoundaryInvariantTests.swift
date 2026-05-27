import XCTest
@testable import Watermelon

final class V2RepoBoundaryInvariantTests: XCTestCase {
    private let basePath = "/repo"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let month = LibraryMonthKey(year: 2026, month: 5)

    func testVersionManifestRequiredFormatRejectsMalformedValues() async throws {
        for value in [nil, "true", "1.5", "9223372036854775808"] {
            let client = try await makeConnectedClient()
            let store = VersionManifestStore(client: client, basePath: basePath)
            var fields: [String: String] = [
                "min_app_version": #""2.0.0""#,
                "created_by_writer": #""writer-A""#
            ]
            if let value { fields["format_version"] = value }
            await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), data: Data(json(fields).utf8))

            do {
                _ = try await store.load()
                XCTFail("expected unreadable format_version for \(String(describing: value))")
            } catch RepoBootstrap.VersionConflict.unreadable {
            }
        }

        let client = try await makeConnectedClient()
        let store = VersionManifestStore(client: client, basePath: basePath)
        await client.injectFile(
            path: RepoLayout.versionFilePath(base: basePath),
            data: Data(json([
                "format_version": "-1",
                "min_app_version": #""2.0.0""#,
                "created_by_writer": #""writer-A""#
            ]).utf8)
        )
        do {
            try await store.verifyCompatible()
            XCTFail("expected negative format_version to reject through compatibility classification")
        } catch RepoBootstrap.VersionConflict.mismatchedFormatVersion {
        }
    }

    func testVersionManifestOptionalCreatedAtMsMalformedValuesResolveNil() async throws {
        for value in [nil, "true", "1.5", "9223372036854775808"] {
            let client = try await makeConnectedClient()
            let store = VersionManifestStore(client: client, basePath: basePath)
            var fields: [String: String] = [
                "format_version": "\(RepoLayout.formatVersion)",
                "min_app_version": #""2.0.0""#,
                "created_by_writer": #""writer-A""#
            ]
            if let value { fields["created_at_ms"] = value }
            await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), data: Data(json(fields).utf8))

            guard case .found(let manifest) = try await store.load() else {
                XCTFail("expected found manifest")
                return
            }
            XCTAssertNil(manifest.createdAtMs, "created_at_ms \(String(describing: value)) must not coerce into a number")
        }
    }

    func testMigrationMarkerRequiredPhaseAndOptionalTimestampsFollowBoundaryRules() throws {
        let filename = "\(writerA).json"

        let missingPhase = try MigrationMarker.parse(
            filename: filename,
            bytes: Data(json(["writer_id": #""\#(writerA)""#]).utf8)
        )
        XCTAssertEqual(missingPhase.phase, .phase1)

        for value in ["true", "1.5", "-1", "9223372036854775808"] {
            let bytes = Data(json([
                "writer_id": #""\#(writerA)""#,
                "phase": value
            ]).utf8)
            XCTAssertThrowsError(try MigrationMarker.parse(filename: filename, bytes: bytes))
        }

        for key in ["started_at_ms", "last_step_at_ms"] {
            for value in ["true", "1.5", "9223372036854775808"] {
                let parsed = try MigrationMarker.parse(
                    filename: filename,
                    bytes: Data(json([
                        "writer_id": #""\#(writerA)""#,
                        "phase": "2",
                        key: value
                    ]).utf8)
                )
                if key == "started_at_ms" {
                    XCTAssertNil(parsed.startedAtMs, "\(key) \(value) must not coerce into a number")
                } else {
                    XCTAssertNil(parsed.lastStepMs, "\(key) \(value) must not coerce into a number")
                }
            }
        }
    }

    func testIdentityClaimCreatedAtMsRequiredAcrossAuthorityPaths() async throws {
        let malformedCreatedAtValues: [String?] = [nil, "true", "1.5", "-1", "9223372036854775808"]
        for value in malformedCreatedAtValues {
            let client = try await makeConnectedClient()
            let store = IdentityClaimStore(client: client, basePath: basePath)
            await injectIdentityClaim(client, writerID: writerB, repoID: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", createdAtMsJSON: value)
            do {
                _ = try await store.canonicalElection(ignoringCorruptSelfClaimFor: writerA)
                XCTFail("expected corrupt foreign claim for \(String(describing: value))")
            } catch RepoBootstrap.BootstrapError.ioFailure {
            }
        }

        for value in malformedCreatedAtValues {
            let client = try await makeConnectedClient()
            let store = IdentityClaimStore(client: client, basePath: basePath)
            await injectIdentityClaim(client, writerID: writerA, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", createdAtMsJSON: value)
            let claim = try await store.readOwnClaim(writerID: writerA)
            XCTAssertNil(claim, "readOwnClaim must not accept \(String(describing: value)) as a timestamp")
        }

        for value in malformedCreatedAtValues {
            let client = try await makeConnectedClient()
            let store = IdentityClaimStore(client: client, basePath: basePath)
            await injectIdentityClaim(client, writerID: writerA, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", createdAtMsJSON: value)
            try await store.writeOwnClaim(repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: writerA, createdAtMs: 123)
            let repaired = try await store.readOwnClaim(writerID: writerA)
            let claim = try XCTUnwrap(repaired)
            XCTAssertEqual(claim.createdAtMs, 123, "writeOwnClaim must repair \(String(describing: value)) instead of preserving it")
        }

        for value in malformedCreatedAtValues {
            let client = try await makeConnectedClient()
            await client.setAtomicCreateMode(.bestEffort)
            let store = IdentityClaimStore(client: client, basePath: basePath)
            let path = RepoLayout.identityClaimPath(base: basePath, writerID: writerA)
            var fields: [String: String] = [
                "v": "1",
                "repo_id": #""aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee""#,
                "writer_id": #""\#(writerA)""#
            ]
            if let value { fields["created_at_ms"] = value }
            await client.stageBestEffortRace(at: path, with: Data(json(fields).utf8))
            do {
                try await store.writeOwnClaim(repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: writerA, createdAtMs: 123)
                XCTFail("expected malformed ambiguous write readback for \(String(describing: value))")
            } catch RepoBootstrap.BootstrapError.ioFailure {
            }
        }
    }

    func testCommitWireRequiredNumericsRejectMalformedValues() {
        let headerInvalids: [String: [String?]] = [
            "v": [nil, "true", "1.5", "-1", "9223372036854775808"],
            "seq": [nil, "true", "1.5", "-1", "18446744073709551616"],
            "clockMin": [nil, "true", "1.5", "-1", "18446744073709551616", "0"],
            "clockMax": [nil, "true", "1.5", "-1", "18446744073709551616", "0"]
        ]
        for (field, values) in headerInvalids {
            for value in values {
                XCTAssertThrowsError(try CommitOpMapper.decodeLine(commitHeaderJSON(replacing: field, with: value)))
            }
        }

        let opInvalids: [String: [String?]] = [
            "opSeq": [nil, "true", "1.5", "-1", "9223372036854775808"],
            "clock": [nil, "true", "1.5", "-1", "18446744073709551616", "0"]
        ]
        for (field, values) in opInvalids {
            for value in values {
                XCTAssertThrowsError(try CommitOpMapper.decodeLine(commitTombstoneJSON(replacing: field, with: value)))
            }
        }

        for value in [nil, "true", "1.5", "-1", "18446744073709551616"] {
            XCTAssertThrowsError(try CommitOpMapper.decodeLine(commitTombstoneBasisJSON(lamport: value, perWriterSeq: "1")))
        }
        for value in ["true", "1.5", "-1", "18446744073709551616"] {
            XCTAssertThrowsError(try CommitOpMapper.decodeLine(commitTombstoneBasisJSON(lamport: "1", perWriterSeq: value)))
        }
    }

    func testSnapshotWireRequiredNumericsRejectMalformedValues() {
        for value in [nil, "true", "1.5", "-1", "9223372036854775808"] {
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotHeaderJSON(version: value)))
        }
        for value in [nil, "true", "1.5", "-1", "18446744073709551616"] {
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotHeaderJSON(coveredLow: value, coveredHigh: "1")))
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotHeaderJSON(coveredLow: "1", coveredHigh: value)))
        }
        for value in [nil, "true", "1.5", "-1", "18446744073709551616", "0"] {
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotAssetJSON(lastSeq: value, lastClock: "1")))
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotAssetJSON(lastSeq: "1", lastClock: value)))
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotResourceJSON(fileSize: "1", lastSeq: value, lastClock: "1")))
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotResourceJSON(fileSize: "1", lastSeq: "1", lastClock: value)))
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotDeletedKeyJSON(lastSeq: value, lastClock: "1")))
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotDeletedKeyJSON(lastSeq: "1", lastClock: value)))
        }
        for value in [nil, "true", "1.5", "-1", "9223372036854775808"] {
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotResourceJSON(fileSize: value)))
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotAssetJSON(resourceCount: value)))
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(snapshotAssetJSON(totalFileSizeBytes: value)))
        }
    }

    func testRepoLayoutFilenameNumericBoundaries() {
        XCTAssertEqual(
            RepoLayout.parseCommitFilename("2026-05--\(writerA)--ffffffffffffffff.jsonl")?.seq,
            UInt64.max
        )
        XCTAssertEqual(
            RepoLayout.parseSnapshotFilename("2026-05--ffffffffffffffff--\(writerA)--run001.jsonl")?.lamport,
            UInt64.max
        )

        for seq in ["", "zzzz", "1.5", "-1", "10000000000000000", "0000000000000000"] {
            XCTAssertNil(RepoLayout.parseCommitFilename("2026-05--\(writerA)--\(seq).jsonl"))
        }
        for lamport in ["", "zzzz", "1.5", "-1", "10000000000000000", "0000000000000000"] {
            XCTAssertNil(RepoLayout.parseSnapshotFilename("2026-05--\(lamport)--\(writerA)--run001.jsonl"))
        }
    }

    func testRetentionManifestStoreFilenameRejectsZeroLamport() {
        let validRef = RetentionManifestRef(
            month: month, lamport: 1, writerID: writerA, runIDPrefix: "aabbcc"
        )
        XCTAssertNotNil(RetentionManifestStore.parseFilename(RetentionManifestStore.filename(for: validRef)))

        let zeroRef = RetentionManifestRef(
            month: month, lamport: 0, writerID: writerA, runIDPrefix: "aabbcc"
        )
        XCTAssertNil(RetentionManifestStore.parseFilename(RetentionManifestStore.filename(for: zeroRef)))
    }

    func testRetentionManifestDecodingRejectsZeroBarrierLamport() throws {
        let json = retentionManifestBarrierLamportJSON(barrierLamport: "0000000000000000")
        do {
            _ = try RetentionManifestStore.decode(Data(json.utf8))
            XCTFail("expected barrier_lamport 0 to be rejected")
        } catch RetentionManifestError.malformed("barrier_lamport") {
        }
    }

    func testMaterializationIsIdempotentForUnchangedRemoteMetadata() async throws {
        let client = try await makeConnectedClient()
        try await writeCommit(client: client, writerID: writerA, seq: 1, clock: 1, fingerprintByte: 0x11)
        try await writeCommit(client: client, writerID: writerA, seq: 2, clock: 2, fingerprintByte: 0x12)

        let first = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let second = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        assertMaterializedEqual(first, second)
    }

    func testMaterializerFanoutHonorsSerialOnlyWrapping() async throws {
        let inner = try await makeConnectedClient()
        let snapshotWriter = SnapshotWriter(client: inner, basePath: basePath)
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: .empty
            ),
            assets: [],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 1,
            runID: "serial-snapshot",
            respectTaskCancellation: false
        )
        for seq in UInt64(1)...UInt64(4) {
            try await writeCommit(
                client: inner,
                writerID: writerA,
                seq: seq,
                clock: seq,
                fingerprintByte: UInt8(0x40 + Int(seq))
            )
        }
        let serialOnly = SerialOnlyOperationProbeClient(inner: inner)

        let output = try await RepoMaterializer(client: serialOnly, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertEqual(output.observedSeqByWriter[writerA], 4)
        // Materializer LISTs three subdirectories now: snapshots/, commits/, and the new
        // index/ (U02 cross-repo index discovery). The empty index/ directory triggers one
        // metadata fallback via RepoJSONLDirectoryListing's not-found path.
        XCTAssertEqual(serialOnly.listCount(), 3)
        XCTAssertEqual(serialOnly.metadataCount(), 1)
        XCTAssertEqual(serialOnly.downloadCount(), 5)
        XCTAssertEqual(serialOnly.maxConcurrentDownloads(), 1)
        XCTAssertEqual(serialOnly.maxConcurrentOperations(), 1)
    }

    func testMaterializerAcceptedCeilingCommitDoesNotMaskValidLowerSeq() async throws {
        let client = try await makeConnectedClient()
        try await writeCommit(client: client, writerID: writerA, seq: 5, clock: 1, fingerprintByte: 0x11)
        try await writeCommit(client: client, writerID: writerA, seq: RepoStateAuthority.maxPersistableSeq, clock: 2, fingerprintByte: 0x12)

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(output.observedSeqByWriter[writerA], 5,
                       "accepted ceiling commit must not mask the valid lower same-writer high-water")
    }

    func testMaterializerCeilingSeqFilenameDoesNotMaskValidLowerSeq() async throws {
        let client = try await makeConnectedClient()
        try await writeCommit(client: client, writerID: writerA, seq: 5, clock: 1, fingerprintByte: 0x11)

        let ceilingSeq = RepoStateAuthority.maxPersistableSeq
        let ceilingFilename = RepoLayout.commitFileName(month: month, writerID: writerA, seq: ceilingSeq)
        let ceilingPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: ceilingSeq)
        await client.injectFile(path: ceilingPath, data: Data("garbage".utf8))

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(output.observedSeqByWriter[writerA], 5,
                       "ceiling seq filename must not mask the valid lower seq from the accepted commit")
    }

    func testMaterializerCoveredRangeCeilingDoesNotMaskSubCeilingHighWater() async throws {
        let client = try await makeConnectedClient()
        let ceiling = RepoStateAuthority.maxPersistableSeq

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 5))
        covered.add(writerID: writerA, range: ClosedSeqRange(low: ceiling, high: ceiling))

        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered
            ),
            assets: [],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 1,
            runID: "covered-ceiling-test",
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(output.observedSeqByWriter[writerA], 5,
                       "covered ceiling range must not mask the valid sub-ceiling high-water when commit files are absent")
    }

    func testSnapshotBaselinePlusUncoveredCommitsMatchesGenesisReplay() async throws {
        let snapshotClient = try await makeConnectedClient()
        let genesisClient = try await makeConnectedClient()

        let baseFP = TestFixtures.assetFingerprint(0x21)
        let baseHash = TestFixtures.fingerprint(0x23)
        let deletedFP = TestFixtures.assetFingerprint(0x24)
        let laterFP = TestFixtures.assetFingerprint(0x25)
        let laterHash = TestFixtures.fingerprint(0x26)
        let baseResource = commitResource(
            path: "2026/05/base.jpg",
            logicalName: "base.jpg",
            hash: baseHash,
            fileSize: 11
        )
        let laterResource = commitResource(
            path: "2026/05/later.jpg",
            logicalName: "later.jpg",
            hash: laterHash,
            fileSize: 17
        )
        let coveredOps = [
            addAssetOp(clock: 1, fingerprint: baseFP, resources: [baseResource]),
            tombstoneOp(clock: 2, fingerprint: deletedFP)
        ]
        let uncoveredOps = [
            addAssetOp(clock: 3, fingerprint: laterFP, resources: [laterResource])
        ]
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        let snapshotWriter = SnapshotWriter(client: snapshotClient, basePath: basePath)
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered
            ),
            assets: [
                SnapshotAssetRow(
                    assetFingerprint: baseFP,
                    creationDateMs: nil,
                    backedUpAtMs: 1,
                    resourceCount: 1,
                    totalFileSizeBytes: 11,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
                )
            ],
            resources: [
                SnapshotResourceRow(
                    physicalRemotePath: baseResource.physicalRemotePath,
                    contentHash: baseHash,
                    fileSize: baseResource.fileSize,
                    resourceType: baseResource.resourceType,
                    creationDateMs: nil,
                    backedUpAtMs: 1,
                    crypto: nil,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
                )
            ],
            assetResources: [
                SnapshotAssetResourceRow(
                    assetFingerprint: baseFP,
                    role: baseResource.role,
                    slot: baseResource.slot,
                    resourceHash: baseHash,
                    logicalName: baseResource.logicalName
                )
            ],
            deletedKeys: [
                SnapshotDeletedKeyRow(
                    keyType: .asset,
                    keyValue: deletedFP.rawValue.hexString,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 2)
                )
            ],
            month: month,
            lamport: 2,
            runID: "snapshot-run",
            respectTaskCancellation: false
        )
        try await writeCommit(client: snapshotClient, writerID: writerA, seq: 1, clockMin: 1, clockMax: 2, ops: coveredOps)
        try await writeCommit(client: snapshotClient, writerID: writerA, seq: 2, clockMin: 3, clockMax: 3, ops: uncoveredOps)

        try await writeCommit(client: genesisClient, writerID: writerA, seq: 1, clockMin: 1, clockMax: 2, ops: coveredOps)
        try await writeCommit(client: genesisClient, writerID: writerA, seq: 2, clockMin: 3, clockMax: 3, ops: uncoveredOps)

        let fromSnapshot = try await RepoMaterializer(client: snapshotClient, basePath: basePath).materialize(expectedRepoID: repoID)
        let fromGenesis = try await RepoMaterializer(client: genesisClient, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(fromSnapshot.state.months[month])

        XCTAssertEqual(fromSnapshot.state, fromGenesis.state)
        XCTAssertEqual(fromSnapshot.observedSeqByWriter, fromGenesis.observedSeqByWriter)
        XCTAssertEqual(fromSnapshot.state.observedClock, fromGenesis.state.observedClock)
        XCTAssertTrue((fromSnapshot.coveredByMonth[month] ?? .empty).superset(of: fromGenesis.coveredByMonth[month] ?? .empty))
        XCTAssertEqual(monthState.resources[baseResource.physicalRemotePath]?.contentHash, baseHash)
        XCTAssertEqual(monthState.assetResources[AssetResourceKey(assetFingerprint: baseFP, role: baseResource.role, slot: baseResource.slot)]?.resourceHash, baseHash)
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(deletedFP))
        XCTAssertEqual(monthState.deletedAssetStamps[deletedFP], OpStamp(writerID: writerA, seq: 1, clock: 2))
    }

    func testCancellationClassificationIsUniformAcrossCommitAndSnapshotWriters() async throws {
        let commitClient = try await makeConnectedClient()
        commitClient.setAtomicCreateGuarantee(.exclusive)
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await commitClient.injectAtomicCreateURLErrorCancelled(for: commitPath)
        do {
            try await writeCommit(client: commitClient, writerID: writerA, seq: 1, clock: 1, fingerprintByte: 0x31)
            XCTFail("expected commit writer cancellation")
        } catch is CancellationError {
        }

        let snapshotClient = try await makeConnectedClient()
        snapshotClient.setAtomicCreateGuarantee(.exclusive)
        let snapshotPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: 1,
            writerID: writerA,
            runID: "snapshot-run"
        )
        await snapshotClient.injectAtomicCreateURLErrorCancelled(for: snapshotPath)
        do {
            _ = try await SnapshotWriter(client: snapshotClient, basePath: basePath).write(
                header: SnapshotHeader(
                    version: SnapshotHeader.currentVersion,
                    scope: CommitHeader.monthScope(month),
                    writerID: writerA,
                    repoID: repoID,
                    covered: .empty
                ),
                assets: [],
                resources: [],
                assetResources: [],
                deletedKeys: [],
                month: month,
                lamport: 1,
                runID: "snapshot-run",
                respectTaskCancellation: false
            )
            XCTFail("expected snapshot writer cancellation")
        } catch is CancellationError {
        }
    }

    func testPresenceAndFreshnessFailClosedOnUncertainMetadata() async throws {
        let client = try await makeConnectedClient()
        let versionStore = VersionManifestStore(client: client, basePath: basePath)
        await client.injectMetadataError(.transport, for: RepoLayout.versionFilePath(base: basePath))
        do {
            _ = try await versionStore.load()
            XCTFail("expected version metadata uncertainty to throw")
        } catch {
            assertInjectedTransportError(error)
        }

        let claimStore = IdentityClaimStore(client: client, basePath: basePath)
        await client.injectListError(.transport, for: RepoLayout.identityDirectoryPath(base: basePath))
        do {
            _ = try await claimStore.canonicalElection(ignoringCorruptSelfClaimFor: writerA)
            XCTFail("expected identity list uncertainty to throw")
        } catch {
            assertInjectedTransportError(error)
        }

        let overlayService = RemoteIndexSyncService()
        let resourceHash = TestFixtures.fingerprint(0x3A)
        let resource = RemoteManifestResource(
            year: month.year,
            month: month.month,
            physicalRemotePath: "2026/05/uncertain.jpg",
            contentHash: resourceHash,
            fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        overlayService.makeOptimisticAssetWriter().appendResource(resource)
        overlayService.markPhysicallyMissingV2(month: month, hashes: [resourceHash])

        let handle = try await overlayService.syncOverlayAndCaptureHandle(client: client, basePath: basePath)
        XCTAssertEqual(handle.overlayFreshness, .stale)
        let verified = overlayService.verifiedPhysicallyMissingHashes(for: month)
        XCTAssertNil(verified)
    }

    func testIdentitySourceResolutionOnlyOwnClaimRepairsWipeReuseMismatch() async throws {
        do {
            let client = try await makeConnectedClient()
            try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff")
            await injectIdentityClaim(client, writerID: writerA, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff", createdAtMsJSON: "0")
            try await withTemporaryDatabase { database in
                let identity = RepoIdentity(database: database)
                let profileID = try TestFixtures.insertServerProfile(in: database, writerID: writerA, basePath: basePath, storageType: .webdav)
                try insertRepoState(database, profileID: profileID, repoID: "aaaaaaaa-cccc-dddd-eeee-ffffffffffff", writerID: writerA)

                let resolution = try await RepoIdentityAuthority(
                    context: RepoIdentityAuthorityContext(
                        profileID: profileID,
                        writerID: writerA,
                        basePath: basePath,
                        dataClient: client,
                        identity: identity,
                        format: RemoteFormatCompatibilityService()
                    )
                ).resolve()

                XCTAssertNil(resolution.stored)
                XCTAssertEqual(resolution.remote, "cccccccc-cccc-dddd-eeee-ffffffffffff")
                XCTAssertEqual(resolution.suggested, "cccccccc-cccc-dddd-eeee-ffffffffffff")
            }
        }

        do {
            let client = try await makeConnectedClient()
            try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff")
            await injectIdentityClaim(client, writerID: writerB, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff", createdAtMsJSON: "0")
            try await withTemporaryDatabase { database in
                let identity = RepoIdentity(database: database)
                let profileID = try TestFixtures.insertServerProfile(in: database, writerID: writerA, basePath: basePath, storageType: .webdav)
                try insertRepoState(database, profileID: profileID, repoID: "aaaaaaaa-cccc-dddd-eeee-ffffffffffff", writerID: writerA)

                do {
                    _ = try await RepoIdentityAuthority(
                        context: RepoIdentityAuthorityContext(
                            profileID: profileID,
                            writerID: writerA,
                            basePath: basePath,
                            dataClient: client,
                            identity: identity,
                            format: RemoteFormatCompatibilityService()
                        )
                    ).resolve()
                    XCTFail("expected foreign claim to preserve mismatch")
                } catch BackupV2RuntimeBuildError.repoIdentityMismatch(let stored, let observed) {
                    XCTAssertEqual(stored, "aaaaaaaa-cccc-dddd-eeee-ffffffffffff")
                    XCTAssertEqual(observed, "cccccccc-cccc-dddd-eeee-ffffffffffff")
                }
            }
        }
    }

    func testMetadataCreateOutcomeVerifiedOnlyAfterRemoteBytesMatchLocalPayload() async throws {
        let exclusiveClient = try await makeConnectedClient()
        exclusiveClient.setAtomicCreateGuarantee(.exclusive)
        let exclusivePayload = try makeTempFile(contents: "exclusive-payload")
        defer { try? FileManager.default.removeItem(at: exclusivePayload) }
        let exclusiveOutcome = try await MetadataCreateGate.createWithStagingFallbackOutcome(
            client: exclusiveClient,
            localURL: exclusivePayload,
            remotePath: "/repo/.watermelon/exclusive.json",
            respectTaskCancellation: false
        )
        XCTAssertEqual(exclusiveOutcome.result, .created)
        try await assertVerifiedOutcomeMeansRemoteBytesMatch(
            exclusiveOutcome,
            client: exclusiveClient,
            path: "/repo/.watermelon/exclusive.json",
            expected: "exclusive-payload"
        )

        let stagingClient = try await makeConnectedClient()
        await stagingClient.setAtomicCreateMode(.bestEffort)
        stagingClient.setMoveIfAbsentGuarantee(.exclusive)
        let stagedPayload = try makeTempFile(contents: "staged-payload")
        defer { try? FileManager.default.removeItem(at: stagedPayload) }
        let stagedOutcome = try await MetadataCreateGate.createWithStagingFallbackOutcome(
            client: stagingClient,
            localURL: stagedPayload,
            remotePath: "/repo/.watermelon/staged.json",
            respectTaskCancellation: false
        )
        XCTAssertEqual(stagedOutcome.result, .created)
        XCTAssertEqual(stagedOutcome.verification, .verifiedLocalBytes)
        try await assertVerifiedOutcomeMeansRemoteBytesMatch(
            stagedOutcome,
            client: stagingClient,
            path: "/repo/.watermelon/staged.json",
            expected: "staged-payload"
        )

        let alreadyExistsClient = try await makeConnectedClient()
        alreadyExistsClient.setAtomicCreateGuarantee(.exclusive)
        let matchingPayload = try makeTempFile(contents: "matching-payload")
        defer { try? FileManager.default.removeItem(at: matchingPayload) }
        await alreadyExistsClient.injectFile(path: "/repo/.watermelon/matching.json", data: Data("matching-payload".utf8))
        let matchingOutcome = try await MetadataCreateGate.createWithStagingFallbackOutcome(
            client: alreadyExistsClient,
            localURL: matchingPayload,
            remotePath: "/repo/.watermelon/matching.json",
            respectTaskCancellation: false
        )
        XCTAssertEqual(matchingOutcome.result, .created)
        XCTAssertEqual(matchingOutcome.verification, .verifiedLocalBytes)
        try await assertVerifiedOutcomeMeansRemoteBytesMatch(
            matchingOutcome,
            client: alreadyExistsClient,
            path: "/repo/.watermelon/matching.json",
            expected: "matching-payload"
        )
    }

    func testLoadSeededListsRemoteDirectorySoOrphanFilesRemainVisible() async throws {
        let client = try await makeConnectedClient()
        await client.injectFile(path: "/repo/2026/05/orphan.jpg", data: Data(repeating: 0xAA, count: 17))

        let store = try await MonthManifestStore.loadSeeded(
            client: client,
            basePath: basePath,
            year: 2026,
            month: 5,
            seed: MonthManifestStore.Seed(resources: [], assets: [], assetResourceLinks: [])
        )

        XCTAssertTrue(store.existingFileNames().contains("orphan.jpg"))
        XCTAssertEqual(store.remoteFileSize(named: "orphan.jpg"), 17)
    }

    func testConcurrentStateAllocationDoesNotDuplicateSeqOrRegressClockHighWater() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        let database = try DatabaseManager(databaseURL: dir.appendingPathComponent("test.sqlite"))
        let profileID = try TestFixtures.insertServerProfile(in: database, writerID: writerA, basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: database)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerA)

        let allocations = try await withThrowingTaskGroup(of: UInt64.self) { group in
            for _ in 0..<32 {
                group.addTask {
                    let allocator = SeqAllocator(database: database, profileID: profileID, repoID: self.repoID, initial: 0)
                    return try await allocator.allocate()
                }
            }
            var values: [UInt64] = []
            for try await value in group {
                values.append(value)
            }
            return values
        }

        XCTAssertEqual(Set(allocations).count, allocations.count)
        XCTAssertGreaterThanOrEqual(allocations.min() ?? 0, 1)
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: repoID)
        XCTAssertEqual(reloaded?.lastSeq, Int64(allocations.max() ?? 0))

        let observer = PersistedLamportClock(database: database, profileID: profileID, repoID: repoID, initial: 0)
        try await observer.observe(500)
        let ranges = try await withThrowingTaskGroup(of: LamportClock.Range.self) { group in
            for _ in 0..<8 {
                group.addTask {
                    let clock = PersistedLamportClock(database: database, profileID: profileID, repoID: self.repoID, initial: 0)
                    return try await clock.tickRange(count: 2)
                }
            }
            var values: [LamportClock.Range] = []
            for try await value in group {
                values.append(value)
            }
            return values
        }
        let emitted = ranges.flatMap { Array($0.low...$0.high) }
        XCTAssertEqual(Set(emitted).count, emitted.count)
        XCTAssertGreaterThanOrEqual(emitted.min() ?? 0, 501)
        let reloadedAfterClock = try await identity.loadRepoState(profileID: profileID, repoID: repoID)
        XCTAssertEqual(reloadedAfterClock?.lastClock, Int64(emitted.max() ?? 0))
    }

    private func makeConnectedClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        return client
    }

    private func withTemporaryDatabase(_ body: (DatabaseManager) async throws -> Void) async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        var caught: Error?
        do {
            let database = try DatabaseManager(databaseURL: dir.appendingPathComponent("test.sqlite"))
            do {
                try await body(database)
            } catch {
                caught = error
            }
        } catch {
            caught = error
        }
        try? FileManager.default.removeItem(at: dir)
        if let caught {
            throw caught
        }
    }

    private func insertRepoState(
        _ database: DatabaseManager,
        profileID: Int64,
        repoID: String,
        writerID: String
    ) throws {
        try database.write { db in
            try RepoStateRecord(
                profileID: profileID,
                repoID: repoID,
                writerID: writerID,
                lastClock: 500,
                lastSeq: 200,
                migrationCompleted: 1
            ).insert(db)
        }
    }

    private func injectIdentityClaim(
        _ client: InMemoryRemoteStorageClient,
        writerID: String,
        repoID: String,
        createdAtMsJSON: String?
    ) async {
        var fields: [String: String] = [
            "v": "1",
            "repo_id": #""\#(repoID)""#,
            "writer_id": #""\#(writerID)""#
        ]
        if let createdAtMsJSON { fields["created_at_ms"] = createdAtMsJSON }
        await client.injectFile(
            path: RepoLayout.identityClaimPath(base: basePath, writerID: writerID),
            data: Data(json(fields).utf8)
        )
    }

    private func writeCommit(
        client: InMemoryRemoteStorageClient,
        writerID: String,
        seq: UInt64,
        clock: UInt64,
        fingerprintByte: UInt8
    ) async throws {
        let op = addAssetOp(
            clock: clock,
            fingerprint: TestFixtures.assetFingerprint(fingerprintByte),
            resources: []
        )
        try await writeCommit(client: client, writerID: writerID, seq: seq, clockMin: clock, clockMax: clock, ops: [op])
    }

    private func writeCommit(
        client: InMemoryRemoteStorageClient,
        writerID: String,
        seq: UInt64,
        clockMin: UInt64,
        clockMax: UInt64,
        ops: [CommitOp]
    ) async throws {
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerID,
                seq: seq,
                runID: "run-\(seq)",
                month: month,
                clockMin: clockMin,
                clockMax: clockMax
            ),
            ops: ops,
            month: month,
            respectTaskCancellation: false
        )
    }

    private func addAssetOp(clock: UInt64, fingerprint: AssetFingerprint, resources: [CommitResourceEntry]) -> CommitOp {
        CommitOp(opSeq: 0, clock: clock, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fingerprint,
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: resources
        )))
    }

    private func tombstoneOp(clock: UInt64, fingerprint: AssetFingerprint) -> CommitOp {
        CommitOp(opSeq: 1, clock: clock, body: .tombstoneAsset(CommitTombstoneBody(
            assetFingerprint: fingerprint,
            reason: .userDeleted
        )))
    }

    private func commitResource(path: String, logicalName: String, hash: Data, fileSize: Int64) -> CommitResourceEntry {
        CommitResourceEntry(
            physicalRemotePath: path,
            logicalName: logicalName,
            contentHash: hash,
            fileSize: fileSize,
            resourceType: ResourceTypeCode.photo,
            role: ResourceTypeCode.photo,
            slot: 0,
            crypto: nil
        )
    }

    private func assertInjectedTransportError(
        _ error: Error,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard case RemoteStorageClientError.underlying(let underlying as NSError) = error else {
            XCTFail("expected injected transport error, got \(error)", file: file, line: line)
            return
        }
        XCTAssertEqual(underlying.domain, NSURLErrorDomain, file: file, line: line)
        XCTAssertEqual(underlying.code, NSURLErrorNotConnectedToInternet, file: file, line: line)
    }

    private func assertVerifiedOutcomeMeansRemoteBytesMatch(
        _ outcome: MetadataCreateGate.CreateOutcome,
        client: InMemoryRemoteStorageClient,
        path: String,
        expected: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        guard outcome.verification == .verifiedLocalBytes else { return }
        let temp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: path, localURL: temp)
        let bytes = try String(decoding: Data(contentsOf: temp), as: UTF8.self)
        XCTAssertEqual(bytes, expected, file: file, line: line)
    }

    private func assertMaterializedEqual(
        _ lhs: RepoMaterializer.MaterializeOutput,
        _ rhs: RepoMaterializer.MaterializeOutput,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertEqual(lhs.state, rhs.state, file: file, line: line)
        XCTAssertEqual(lhs.observedSeqByWriter, rhs.observedSeqByWriter, file: file, line: line)
        XCTAssertEqual(lhs.coveredByMonth, rhs.coveredByMonth, file: file, line: line)
        XCTAssertEqual(lhs.corruptedSnapshotMonths, rhs.corruptedSnapshotMonths, file: file, line: line)
    }

    private func commitHeaderJSON(replacing field: String, with value: String?) -> String {
        var fields: [String: String] = [
            "t": #""header""#,
            "v": "1",
            "repoID": #""aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee""#,
            "writerID": #""w""#,
            "seq": "1",
            "runID": #""run""#,
            "scope": #""month:2026-05""#,
            "clockMin": "1",
            "clockMax": "1",
            "bodyKind": #""plain""#
        ]
        fields[field] = value
        return json(fields)
    }

    private func commitTombstoneJSON(replacing field: String, with value: String?) -> String {
        var fields: [String: String] = [
            "t": #""op""#,
            "opSeq": "0",
            "clock": "1",
            "kind": #""tombstoneAsset""#,
            "body": #"{"assetFingerprint":"\#(TestFixtures.fingerprint(0xAA).hexString)","reason":"userDeleted"}"#
        ]
        fields[field] = value
        return json(fields)
    }

    private func commitTombstoneBasisJSON(lamport: String?, perWriterSeq: String) -> String {
        var basis: [String: String] = [
            "perWriterMaxSeq": #"{"writer-A":\#(perWriterSeq)}"#
        ]
        if let lamport { basis["lamportWatermark"] = lamport }
        let body = #"{"assetFingerprint":"\#(TestFixtures.fingerprint(0xAB).hexString)","reason":"verifyFailed","observedBasis":\#(json(basis))}"#
        return json([
            "t": #""op""#,
            "opSeq": "0",
            "clock": "1",
            "kind": #""tombstoneAsset""#,
            "body": body
        ])
    }

    private func snapshotHeaderJSON(
        version: String? = "1",
        coveredLow: String? = "1",
        coveredHigh: String? = "1"
    ) -> String {
        var fields: [String: String] = [
            "t": #""header""#,
            "scope": #""month:2026-05""#,
            "writerID": #""w""#,
            "repoID": #""aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee""#
        ]
        if let version { fields["v"] = version }
        if let coveredLow, let coveredHigh {
            fields["covered"] = #"{"writer-A":[[\#(coveredLow),\#(coveredHigh)]]}"#
        }
        return json(fields)
    }

    private func snapshotAssetJSON(
        resourceCount: String? = "1",
        totalFileSizeBytes: String? = "1",
        lastSeq: String? = nil,
        lastClock: String? = nil
    ) -> String {
        var row: [String: String] = [
            "assetFingerprint": #""\#(TestFixtures.fingerprint(0xAC).hexString)""#,
            "creationDateMs": "null",
            "backedUpAtMs": "1"
        ]
        if let resourceCount { row["resourceCount"] = resourceCount }
        if let totalFileSizeBytes { row["totalFileSizeBytes"] = totalFileSizeBytes }
        if lastSeq != nil || lastClock != nil {
            row["lastWriterID"] = #""\#(writerA)""#
            if let lastSeq { row["lastSeq"] = lastSeq }
            if let lastClock { row["lastClock"] = lastClock }
        }
        return json(["t": #""asset""#, "r": json(row)])
    }

    private func snapshotResourceJSON(
        fileSize: String?,
        lastSeq: String? = nil,
        lastClock: String? = nil
    ) -> String {
        var row: [String: String] = [
            "physicalRemotePath": #""2026/05/photo.jpg""#,
            "contentHash": #""\#(TestFixtures.fingerprint(0xAD).hexString)""#,
            "resourceType": "1",
            "creationDateMs": "null",
            "backedUpAtMs": "1",
            "crypto": "null"
        ]
        if let fileSize { row["fileSize"] = fileSize }
        if lastSeq != nil || lastClock != nil {
            row["lastWriterID"] = #""\#(writerA)""#
            if let lastSeq { row["lastSeq"] = lastSeq }
            if let lastClock { row["lastClock"] = lastClock }
        }
        return json(["t": #""resource""#, "r": json(row)])
    }

    private func snapshotDeletedKeyJSON(lastSeq: String?, lastClock: String?) -> String {
        var row: [String: String] = [
            "keyType": #""asset""#,
            "keyValue": #""\#(TestFixtures.fingerprint(0xAE).hexString)""#
        ]
        if lastSeq != nil || lastClock != nil {
            row["lastWriterID"] = #""\#(writerA)""#
            if let lastSeq { row["lastSeq"] = lastSeq }
            if let lastClock { row["lastClock"] = lastClock }
        }
        return json(["t": #""deleted_key""#, "r": json(row)])
    }

    private func json(_ fields: [String: String]) -> String {
        "{" + fields.map { #""\#($0.key)":\#($0.value)"# }.sorted().joined(separator: ",") + "}"
    }

    private func makeTempFile(contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(contents.utf8).write(to: url)
        return url
    }

    private func retentionManifestBarrierLamportJSON(barrierLamport: String) -> String {
        json([
            "version": "1",
            "repo_id": #""aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee""#,
            "month": #""2026-05""#,
            "created_by_writer_id": #""\#(writerA)""#,
            "run_id": #""33333333-3333-3333-3333-333333333333""#,
            "created_at_ms": "1000",
            "barrier_lamport": #""\#(barrierLamport)""#,
            "checkpoint_snapshot": #""placeholder""#,
            "checkpoint_sha256": #""\#(String(repeating: "a", count: 64))""#,
            "covered_ranges": #"{}"#,
            "delete_prefix_by_writer": "{}",
            "observed_seq_high_by_writer": "{}",
            "policy": #"{"keep_uncovered_commits":true,"keep_corrupt_or_untrusted_commits":true,"keep_tombstones":true,"snapshot_keep_count":2}"#,
            "liveness_gate": #"{"required_complete_view":true,"required_no_active_non_self_writers":true,"legacy_client_grace_ms":60000}"#
        ])
    }
}

private final class SerialOnlyOperationProbeClient: RemoteStorageClientProtocol, @unchecked Sendable {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .serialOnly }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }

    private let inner: InMemoryRemoteStorageClient
    private let lock = NSLock()
    private var activeOperations = 0
    private var maxOperations = 0
    private var activeDownloads = 0
    private var maxDownloads = 0
    private var totalLists = 0
    private var totalMetadata = 0
    private var totalDownloads = 0

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    func listCount() -> Int {
        lock.lock()
        defer { lock.unlock() }
        return totalLists
    }

    func metadataCount() -> Int {
        lock.lock()
        defer { lock.unlock() }
        return totalMetadata
    }

    func downloadCount() -> Int {
        lock.lock()
        defer { lock.unlock() }
        return totalDownloads
    }

    func maxConcurrentDownloads() -> Int {
        lock.lock()
        defer { lock.unlock() }
        return maxDownloads
    }

    func maxConcurrentOperations() -> Int {
        lock.lock()
        defer { lock.unlock() }
        return maxOperations
    }

    func connect() async throws {
        try await inner.connect()
    }

    func disconnect() async {
        await inner.disconnect()
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        try await inner.storageCapacity()
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        recordOperationStart()
        recordList()
        defer { recordOperationEnd() }
        try await Task.sleep(nanoseconds: 5_000_000)
        return try await inner.list(path: path)
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        recordOperationStart()
        recordMetadata()
        defer { recordOperationEnd() }
        try await Task.sleep(nanoseconds: 5_000_000)
        return try await inner.metadata(path: path)
    }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        try await inner.upload(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        .exclusive
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }

    func download(remotePath: String, localURL: URL) async throws {
        recordOperationStart()
        recordDownloadStart()
        defer {
            recordDownloadEnd()
            recordOperationEnd()
        }
        try await Task.sleep(nanoseconds: 5_000_000)
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }

    func exists(path: String) async throws -> Bool {
        try await inner.exists(path: path)
    }

    func delete(path: String) async throws {
        try await inner.delete(path: path)
    }

    func createDirectory(path: String) async throws {
        try await inner.createDirectory(path: path)
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.move(from: sourcePath, to: destinationPath)
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.copy(from: sourcePath, to: destinationPath)
    }

    private func recordDownloadStart() {
        lock.lock()
        totalDownloads += 1
        activeDownloads += 1
        maxDownloads = max(maxDownloads, activeDownloads)
        lock.unlock()
    }

    private func recordList() {
        lock.lock()
        totalLists += 1
        lock.unlock()
    }

    private func recordMetadata() {
        lock.lock()
        totalMetadata += 1
        lock.unlock()
    }

    private func recordDownloadEnd() {
        lock.lock()
        activeDownloads -= 1
        lock.unlock()
    }

    private func recordOperationStart() {
        lock.lock()
        activeOperations += 1
        maxOperations = max(maxOperations, activeOperations)
        lock.unlock()
    }

    private func recordOperationEnd() {
        lock.lock()
        activeOperations -= 1
        lock.unlock()
    }
}
