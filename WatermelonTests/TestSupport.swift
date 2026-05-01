import Foundation
@testable import Watermelon

final class TestAssetCollection: LibraryAssetCollection, @unchecked Sendable {
    let assetSnapshots: [LibraryAssetSnapshot]

    init(_ snapshots: [LibraryAssetSnapshot]) {
        self.assetSnapshots = snapshots
    }
}

final class TestChangeProvider: LibraryChangeProvider {
    private var changes: [ObjectIdentifier: LibraryCollectionChange] = [:]

    func setChange(for collection: LibraryAssetCollection, _ change: LibraryCollectionChange) {
        changes[ObjectIdentifier(collection)] = change
    }

    func change(for collection: LibraryAssetCollection) -> LibraryCollectionChange? {
        changes[ObjectIdentifier(collection)]
    }
}

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
            localIdentifier: id,
            creationDate: date(year, month, day),
            modificationDate: modificationDate,
            mediaKind: kind
        )
    }

    static func emptyFingerprint(for ids: Set<String>) -> [String: LocalAssetFingerprintRecord] {
        [:]
    }

    static func record(_ fingerprint: Data, updatedAt: Date = Date()) -> LocalAssetFingerprintRecord {
        LocalAssetFingerprintRecord(fingerprint: fingerprint, updatedAt: updatedAt)
    }

    static func emptyRemoteFingerprints(for month: LibraryMonthKey) -> Set<Data> {
        []
    }

    // MARK: - RemoteIndex builders

    static func remoteAsset(
        year: Int,
        month: Int,
        fingerprint: Data,
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
        RemoteManifestResource(
            year: year,
            month: month,
            fileName: fileName ?? contentHash.hexString,
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
        assetFingerprint: Data,
        resourceHash: Data,
        role: Int = ResourceTypeCode.photo,
        slot: Int = 0
    ) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: assetFingerprint,
            resourceHash: resourceHash,
            role: role,
            slot: slot
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
}
