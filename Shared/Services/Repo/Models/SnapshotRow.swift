import Foundation

struct SnapshotHeader: Equatable, Sendable {
    static let currentVersion = 1
    static let checkpointVersion = 2
    let version: Int
    let scope: String
    let writerID: String
    let repoID: String
    let covered: CoveredRanges
    let createdAtMs: Int64?
    /// Present on attested writes; binds this header's covered to the filename digest. Legacy headers
    /// leave it nil and parse/materialize exactly as before.
    let coverageAttestation: SnapshotCoverageAttestation?

    init(
        version: Int,
        scope: String,
        writerID: String,
        repoID: String,
        covered: CoveredRanges,
        createdAtMs: Int64?,
        coverageAttestation: SnapshotCoverageAttestation? = nil
    ) {
        self.version = version
        self.scope = scope
        self.writerID = writerID
        self.repoID = repoID
        self.covered = covered
        self.createdAtMs = createdAtMs
        self.coverageAttestation = coverageAttestation
    }
}

struct SnapshotAssetRow: Equatable, Sendable {
    let assetFingerprint: AssetFingerprint
    let creationDateMs: Int64?
    let backedUpAtMs: Int64
    let resourceCount: Int
    let totalFileSizeBytes: Int64
    let stamp: OpStamp

    init(
        assetFingerprint: AssetFingerprint,
        creationDateMs: Int64?,
        backedUpAtMs: Int64,
        resourceCount: Int,
        totalFileSizeBytes: Int64,
        stamp: OpStamp
    ) {
        self.assetFingerprint = assetFingerprint
        self.creationDateMs = creationDateMs
        self.backedUpAtMs = backedUpAtMs
        self.resourceCount = resourceCount
        self.totalFileSizeBytes = totalFileSizeBytes
        self.stamp = stamp
    }
}

struct SnapshotResourceRow: Equatable, Sendable {
    let physicalRemotePath: String
    let contentHash: Data
    let fileSize: Int64
    let resourceType: Int
    let creationDateMs: Int64?
    let backedUpAtMs: Int64
    let crypto: ResourceCryptoMetadata?
    let stamp: OpStamp

    init(
        physicalRemotePath: String,
        contentHash: Data,
        fileSize: Int64,
        resourceType: Int,
        creationDateMs: Int64?,
        backedUpAtMs: Int64,
        crypto: ResourceCryptoMetadata?,
        stamp: OpStamp
    ) {
        self.physicalRemotePath = physicalRemotePath
        self.contentHash = contentHash
        self.fileSize = fileSize
        self.resourceType = resourceType
        self.creationDateMs = creationDateMs
        self.backedUpAtMs = backedUpAtMs
        self.crypto = crypto
        self.stamp = stamp
    }
}

struct SnapshotAssetResourceRow: Equatable, Sendable {
    let assetFingerprint: AssetFingerprint
    let role: Int
    let slot: Int
    let resourceHash: Data
    let logicalName: String
}

struct SnapshotDeletedKeyRow: Equatable, Sendable {
    enum KeyType: String, Sendable {
        case asset
        case resource
        case assetResource
    }
    let keyType: KeyType
    let keyValue: String
    let stamp: OpStamp

    init(keyType: KeyType, keyValue: String, stamp: OpStamp) {
        self.keyType = keyType
        self.keyValue = keyValue
        self.stamp = stamp
    }
}

enum SnapshotRow: Equatable, Sendable {
    case header(SnapshotHeader)
    case asset(SnapshotAssetRow)
    case resource(SnapshotResourceRow)
    case assetResource(SnapshotAssetResourceRow)
    case deletedKey(SnapshotDeletedKeyRow)
    case end(sha256Hex: String, rowCount: Int)
}

struct SnapshotFile: Equatable, Sendable {
    let header: SnapshotHeader
    let assets: [SnapshotAssetRow]
    let resources: [SnapshotResourceRow]
    let assetResources: [SnapshotAssetResourceRow]
    let deletedKeys: [SnapshotDeletedKeyRow]
    let sha256Hex: String
    let rowCount: Int
}
