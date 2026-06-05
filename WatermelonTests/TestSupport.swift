import Foundation
@testable import Watermelon

enum TestFixtures {
    private static let calendar = Calendar(identifier: .gregorian)

    static func date(_ year: Int, _ month: Int, _ day: Int = 15) -> Date {
        calendar.date(from: DateComponents(year: year, month: month, day: day))!
    }

    static func snapshot(
        id: String,
        year: Int = 2024,
        month: Int = 1,
        day: Int = 15,
        kind: AlbumMediaKind = .photo,
        modificationDate: Date? = nil
    ) -> LibraryAssetSnapshot {
        LibraryAssetSnapshot(
            localIdentifier: PhotoKitLocalIdentifier(rawValue: id),
            creationDate: date(year, month, day),
            modificationDate: modificationDate,
            mediaKind: kind
        )
    }

    static func initialPayload(_ snapshotsPerCollection: [[LibraryAssetSnapshot]]) -> LibraryInitialPayload {
        LibraryInitialPayload(collections: snapshotsPerCollection)
    }

    static func incrementalChange(
        at collectionIndex: Int,
        removed: [String] = [],
        inserted: [LibraryAssetSnapshot] = [],
        changed: [LibraryAssetSnapshot] = [],
        moved: [LibraryAssetSnapshot] = []
    ) -> LibraryChangePayload.CollectionChange {
        .incremental(
            collectionIndex: collectionIndex,
            removed: removed.map { PhotoKitLocalIdentifier(rawValue: $0) },
            inserted: inserted,
            changed: changed,
            moved: moved
        )
    }

    static func nonIncrementalChange(
        at collectionIndex: Int,
        nextSnapshots: [LibraryAssetSnapshot]
    ) -> LibraryChangePayload.CollectionChange {
        .nonIncremental(collectionIndex: collectionIndex, nextSnapshots: nextSnapshots)
    }

    static func changePayload(_ collectionChanges: [LibraryChangePayload.CollectionChange]) -> LibraryChangePayload {
        LibraryChangePayload(collectionChanges: collectionChanges)
    }

    static func emptyFingerprint(for ids: Set<PhotoKitLocalIdentifier>) -> [PhotoKitLocalIdentifier: LocalAssetFingerprintRecord] {
        [:]
    }

    static func record(
        _ fingerprint: AssetFingerprint,
        updatedAt: Date = Date(),
        selectionVersion: Int = BackupAssetResourcePlanner.currentSelectionVersion,
        resourceSignature: Data? = Data()
    ) -> LocalAssetFingerprintRecord {
        LocalAssetFingerprintRecord(
            fingerprint: fingerprint,
            updatedAt: updatedAt,
            selectionVersion: selectionVersion,
            resourceSignature: resourceSignature
        )
    }

    static func emptyRemoteFingerprints(for month: LibraryMonthKey) -> Set<AssetFingerprint> {
        []
    }


    /// Use for any test that exercises the classifier — fake fingerprints trip `.fingerprintMismatch`.
    static func computedFingerprint(for resourceRoleSlotHashes: [(role: Int, slot: Int, contentHash: Data)]) -> AssetFingerprint {
        BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: resourceRoleSlotHashes)
    }

    static func remoteAsset(
        year: Int,
        month: Int,
        fingerprint: AssetFingerprint,
        creationDateMs: Int64? = nil,
        backedUpAtMs: Int64 = 0,
        resourceCount: Int = 1,
        totalFileSizeBytes: Int64 = 100
    ) -> RemoteManifestAsset {
        RemoteManifestAsset(
            year: year,
            month: month,
            assetFingerprint: fingerprint,
            creationDateMs: creationDateMs,
            backedUpAtMs: backedUpAtMs,
            resourceCount: resourceCount,
            totalFileSizeBytes: totalFileSizeBytes
        )
    }

    static func remoteResource(
        year: Int,
        month: Int,
        contentHash: Data,
        fileSize: Int64 = 100,
        resourceType: Int = ResourceTypeCode.photo,
        fileName: String? = nil
    ) -> RemoteManifestResource {
        let leaf = fileName ?? contentHash.hexString
        return RemoteManifestResource(
            year: year,
            month: month,
            physicalRemotePath: String(format: "%04d/%02d/%@", year, month, leaf),
            contentHash: contentHash,
            fileSize: fileSize,
            resourceType: resourceType,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
    }

    static func remoteLink(
        year: Int,
        month: Int,
        assetFingerprint: AssetFingerprint,
        resourceHash: Data,
        role: Int = ResourceTypeCode.photo,
        slot: Int = 0,
        logicalName: String = ""
    ) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: assetFingerprint,
            resourceHash: resourceHash,
            role: role,
            slot: slot,
            logicalName: logicalName
        )
    }

    static func remoteMonthDelta(
        _ key: LibraryMonthKey,
        assets: [RemoteManifestAsset],
        resources: [RemoteManifestResource],
        links: [RemoteAssetResourceLink]
    ) -> RemoteLibraryMonthDelta {
        RemoteLibraryMonthDelta(
            month: key,
            resources: resources,
            assets: assets,
            assetResourceLinks: links
        )
    }

    static func remoteSnapshotState(
        revision: UInt64,
        isFullSnapshot: Bool,
        deltas: [RemoteLibraryMonthDelta]
    ) -> RemoteLibrarySnapshotState {
        RemoteLibrarySnapshotState(
            revision: revision,
            isFullSnapshot: isFullSnapshot,
            monthDeltas: deltas
        )
    }


    /// 32-byte filler used as opaque content/resource-hash bytes; the byte value carries no
    /// semantics, it just makes the value visually distinguishable in failure logs.
    static func fingerprint(_ byte: UInt8) -> Data {
        Data(repeating: byte, count: 32)
    }

    /// Typed asset fingerprint helper for tests; force-unwrap is safe because input is 32 bytes.
    static func assetFingerprint(_ byte: UInt8) -> AssetFingerprint {
        AssetFingerprint(decoding: Data(repeating: byte, count: 32))!
    }

    static func tombstoneBasis(lamportWatermark: UInt64 = 0) -> TombstoneObservationBasis {
        TombstoneObservationBasis(perWriterMaxSeq: [:], lamportWatermark: lamportWatermark)
    }

    static func opStamp(writerID: String = "11111111-1111-1111-1111-aaaaaaaaaaaa", seq: UInt64 = 1, clock: UInt64 = 1) -> OpStamp {
        OpStamp(writerID: writerID, seq: seq, clock: clock)
    }

    static func makeServerProfile(
        id: Int64? = nil,
        name: String = "Test",
        storageType: StorageType = .smb,
        host: String = "h",
        port: Int = 445,
        shareName: String = "s",
        basePath: String = "/p",
        username: String = "u",
        domain: String? = nil,
        writerID: String? = nil,
        backgroundBackupEnabled: Bool = false
    ) -> ServerProfileRecord {
        ServerProfileRecord(
            id: id,
            name: name,
            storageType: storageType.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: host,
            port: port,
            shareName: shareName,
            basePath: basePath,
            username: username,
            domain: domain,
            credentialRef: "ref",
            backgroundBackupEnabled: backgroundBackupEnabled,
            createdAt: Date(),
            updatedAt: Date(),
            writerID: writerID
        )
    }

    @discardableResult
    static func insertServerProfile(
        in databaseManager: DatabaseManager,
        writerID: String? = nil,
        basePath: String = "/p",
        storageType: StorageType = .smb
    ) throws -> Int64 {
        var profile = makeServerProfile(storageType: storageType, basePath: basePath, writerID: writerID)
        try databaseManager.write { db in try profile.save(db) }
        guard let id = profile.id else {
            throw NSError(domain: "TestFixtures", code: -1, userInfo: [NSLocalizedDescriptionKey: "save did not assign id"])
        }
        return id
    }

    static func makeCommitHeader(
        repoID: String,
        writerID: String,
        seq: UInt64,
        runID: String,
        month: LibraryMonthKey,
        clockMin: UInt64? = nil,
        clockMax: UInt64? = nil
    ) -> CommitHeader {
        CommitHeader(
            version: CommitHeader.currentVersion,
            repoID: repoID,
            writerID: writerID,
            seq: seq,
            runID: runID,
            scope: CommitHeader.monthScope(month),
            clockMin: clockMin ?? seq,
            clockMax: clockMax ?? seq,
            bodyKind: CommitHeader.bodyKindPlain
        )
    }

    /// Canonical attested-snapshot path for `header` (digest derived from its coverage attestation).
    static func attestedSnapshotPath(
        basePath: String,
        header: SnapshotHeader,
        month: LibraryMonthKey,
        lamport: UInt64,
        runID: String
    ) -> String {
        let digest = SnapshotCoverageDigest.filenameDigest(
            forHeader: header, month: month, lamport: lamport, runIDPrefix: RepoLayout.runIDPrefix(runID)
        )
        return RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: lamport, writerID: header.writerID, runID: runID, digest: digest
        )
    }

    /// Injects a body-corrupt snapshot whose attested header line is intact at its canonical attested path,
    /// so `SnapshotReader` recovers the authenticated covered while the body still fails integrity. Returns
    /// the injected filename.
    @discardableResult
    static func injectAttestedCorruptSnapshot(
        _ client: InMemoryRemoteStorageClient,
        basePath: String,
        month: LibraryMonthKey,
        writerID: String,
        repoID: String,
        lamport: UInt64,
        runID: String,
        covered: CoveredRanges
    ) async throws -> String {
        let header = SnapshotHeader(
            version: SnapshotHeader.checkpointVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerID,
            repoID: repoID,
            covered: covered,
            createdAtMs: nil,
            coverageAttestation: SnapshotCoverageAttestation()
        )
        let path = attestedSnapshotPath(basePath: basePath, header: header, month: month, lamport: lamport, runID: runID)
        let headerLine = try SnapshotRowMapper.encodeHeaderLine(header)
        await client.injectFile(path: path, contents: headerLine + "\ncorrupt-body-not-jsonl\n")
        return (path as NSString).lastPathComponent
    }

    static func injectIdentityFinalization(
        _ client: InMemoryRemoteStorageClient,
        basePath: String,
        repoID: String,
        writerID: String = "test"
    ) async throws {
        let data = try RepoIdentityFinalizationWire(
            repoID: repoID,
            formatVersion: RepoLayout.formatVersion,
            createdAtMs: 0,
            createdByWriter: writerID
        ).encode()
        await client.injectFile(path: RepoLayout.identityFinalizationFilePath(base: basePath), data: data)
    }

    static func injectVersionJSON(
        _ client: InMemoryRemoteStorageClient,
        basePath: String,
        formatVersion: Int = RepoLayout.formatVersion,
        minAppVersion: String = "2.0.0",
        writerID: String = "test"
    ) async throws {
        let body: [String: Any] = [
            "format_version": formatVersion,
            "min_app_version": minAppVersion,
            "created_at_ms": 0,
            "created_by_writer": writerID
        ]
        let data = try JSONSerialization.data(withJSONObject: body)
        await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), data: data)
    }

    /// V1 manifests live at `<basePath>/<year>/<month>/.watermelon_manifest.sqlite`.
    /// Content doesn't matter for inspect/scan — only file presence.
    static func injectV1ManifestSentinel(
        _ client: InMemoryRemoteStorageClient,
        basePath: String,
        year: Int,
        month: Int
    ) async {
        let path = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", year, month)
        await client.injectFile(path: path, data: Data([0x01]))
    }
}

extension CommitTombstoneBody {
    init(assetFingerprint: AssetFingerprint, reason: Reason) {
        self.init(
            assetFingerprint: assetFingerprint,
            reason: reason,
            observedBasis: TestFixtures.tombstoneBasis(lamportWatermark: 1)
        )
    }
}

extension SnapshotAssetRow {
    init(
        assetFingerprint: AssetFingerprint,
        creationDateMs: Int64?,
        backedUpAtMs: Int64,
        resourceCount: Int,
        totalFileSizeBytes: Int64
    ) {
        self.init(
            assetFingerprint: assetFingerprint,
            creationDateMs: creationDateMs,
            backedUpAtMs: backedUpAtMs,
            resourceCount: resourceCount,
            totalFileSizeBytes: totalFileSizeBytes,
            stamp: TestFixtures.opStamp()
        )
    }
}

extension SnapshotResourceRow {
    init(
        physicalRemotePath: String,
        contentHash: Data,
        fileSize: Int64,
        resourceType: Int,
        creationDateMs: Int64?,
        backedUpAtMs: Int64,
        crypto: ResourceCryptoMetadata?
    ) {
        self.init(
            physicalRemotePath: physicalRemotePath,
            contentHash: contentHash,
            fileSize: fileSize,
            resourceType: resourceType,
            creationDateMs: creationDateMs,
            backedUpAtMs: backedUpAtMs,
            crypto: crypto,
            stamp: TestFixtures.opStamp()
        )
    }
}

extension SnapshotDeletedKeyRow {
    init(keyType: KeyType, keyValue: String) {
        self.init(keyType: keyType, keyValue: keyValue, stamp: TestFixtures.opStamp())
    }
}

extension SnapshotWriter {
    /// Test helper for placeholder baselines: the materializer rejects any snapshot asset row carrying no
    /// link, so for every asset that the caller left link-less this auto-adds a minimal in-month resource
    /// + role/slot link stamped with the asset's own stamp (keeping it inside the same covered range).
    /// Assets already carrying a link, and explicitly-empty asset lists, are passed through unchanged.
    @discardableResult
    func writeBaseline(
        header: SnapshotHeader,
        assets: [SnapshotAssetRow],
        resources: [SnapshotResourceRow],
        assetResources: [SnapshotAssetResourceRow],
        deletedKeys: [SnapshotDeletedKeyRow],
        month: LibraryMonthKey,
        lamport: UInt64,
        runID: String,
        respectTaskCancellation: Bool
    ) async throws -> SnapshotFile {
        var resources = resources
        var assetResources = assetResources
        let linked = Set(assetResources.map(\.assetFingerprint))
        for asset in assets where !linked.contains(asset.assetFingerprint) {
            let hash = asset.assetFingerprint.rawValue
            let token = String(asset.assetFingerprint.rawValue.hexString.prefix(12))
            let path = String(format: "%04d/%02d/auto-\(token).bin", month.year, month.month)
            resources.append(SnapshotResourceRow(
                physicalRemotePath: path,
                contentHash: hash,
                fileSize: 1,
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil,
                backedUpAtMs: 1,
                crypto: nil,
                stamp: asset.stamp
            ))
            assetResources.append(SnapshotAssetResourceRow(
                assetFingerprint: asset.assetFingerprint,
                role: ResourceTypeCode.photo,
                slot: 0,
                resourceHash: hash,
                logicalName: "auto-\(token).bin"
            ))
        }
        return try await write(
            header: header,
            assets: assets,
            resources: resources,
            assetResources: assetResources,
            deletedKeys: deletedKeys,
            month: month,
            lamport: lamport,
            runID: runID,
            respectTaskCancellation: respectTaskCancellation
        )
    }
}
