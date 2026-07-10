import Foundation

// A content-addressed thumbnail sidecar (.watermelon/thumbs/<shard>/<fp>.<codec>) whose fingerprint is
// absent from the authoritative live set (every month manifest's asset fingerprints).
struct ThumbnailOrphan: Sendable, Hashable {
    let fingerprintHex: String
    let storageCodec: Int
    let path: String
    let size: Int64

    var sidecarKey: RemoteThumbnailSidecarKey {
        RemoteThumbnailSidecarKey(fingerprintHex: fingerprintHex, storageCodec: storageCodec)
    }
}

struct ThumbnailOrphanScanResult: Sendable {
    let orphans: [ThumbnailOrphan]

    static let empty = ThumbnailOrphanScanResult(orphans: [])

    var count: Int { orphans.count }
    var totalBytes: Int64 { orphans.reduce(0) { $0 + $1.size } }
}

struct ThumbnailOrphanDeleteResult: Sendable {
    let deletedCount: Int
    let deletedBytes: Int64
    let failedCount: Int

    static let empty = ThumbnailOrphanDeleteResult(deletedCount: 0, deletedBytes: 0, failedCount: 0)
}

// Garbage-collects orphan thumbnail sidecars. An orphan is a `<fp>.jpg` / `<fp>.wmenc` whose fingerprint is not in
// `liveSidecarKeys` — the union of every month manifest's display-resource thumbnail keys. The live set MUST be
// built fail-closed (any manifest-load fault aborts before constructing this scanner): a partial set
// would falsely mark live thumbnails as orphans and delete them. `delete` re-checks membership against
// the (freshly rebuilt) live set so a thumbnail that became live between scan and delete is kept.
//
// "Live" is defined by *flushed* manifests. A sidecar uploaded by a backup that was interrupted before
// its manifest flush is therefore reclaimable here — that's intended: it's a best-effort cache that the
// next backup of that asset regenerates, and the asset's own (also-unflushed) bytes are exactly the
// leftover data files this same cleanup flow reclaims. No authoritative data is lost.
struct ThumbnailOrphanScanner: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String
    let liveSidecarKeys: Set<RemoteThumbnailSidecarKey>

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        liveSidecarKeys: Set<RemoteThumbnailSidecarKey>
    ) {
        self.client = client
        self.basePath = basePath
        self.liveSidecarKeys = liveSidecarKeys
    }

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        liveFingerprintHexes: Set<String>
    ) {
        self.init(
            client: client,
            basePath: basePath,
            liveSidecarKeys: Set(liveFingerprintHexes.flatMap { hex in
                [
                    RemoteThumbnailSidecarKey(fingerprintHex: hex, storageCodec: RemoteManifestResource.plaintextStorageCodec),
                    RemoteThumbnailSidecarKey(fingerprintHex: hex, storageCodec: RemoteManifestResource.encryptedStorageCodec)
                ]
            })
        )
    }

    func scan() async throws -> ThumbnailOrphanScanResult {
        let root = RemoteThumbnailPaths.rootAbsolutePath(basePath: basePath)
        let shards: [RemoteStorageEntry]
        do {
            shards = try await client.list(path: root)
        } catch {
            // No thumbs tree yet (feature never used / already purged) → nothing to clean.
            if RemoteFaultLite.classify(error) == .notFound { return .empty }
            throw error
        }

        var orphans: [ThumbnailOrphan] = []
        for shard in shards where shard.isDirectory {
            try Task.checkCancellation()
            let files = try await client.list(path: shard.path)
            for file in files where !file.isDirectory {
                guard let key = Self.sidecarKey(fromFileName: file.name) else { continue }
                if !liveSidecarKeys.contains(key) {
                    orphans.append(ThumbnailOrphan(
                        fingerprintHex: key.fingerprintHex,
                        storageCodec: key.storageCodec,
                        path: file.path,
                        size: file.size
                    ))
                }
            }
        }
        return ThumbnailOrphanScanResult(orphans: orphans)
    }

    func delete(
        _ targets: [ThumbnailOrphan],
        assertOwnership: MonthManifestOwnershipAssertion?,
        onProgress: (@Sendable (Int, Int) -> Void)? = nil
    ) async throws -> ThumbnailOrphanDeleteResult {
        let total = targets.count
        guard total > 0 else { return .empty }

        var deletedCount = 0
        var deletedBytes: Int64 = 0
        var failedCount = 0

        for (index, target) in targets.enumerated() {
            try Task.checkCancellation()
            // Re-verify membership under the fresh lease: a fingerprint that became live since the scan
            // is kept, not deleted.
            if liveSidecarKeys.contains(target.sidecarKey) {
                failedCount += 1
                onProgress?(index + 1, total)
                continue
            }
            // Prove we still own the write lock immediately before each irreversible delete.
            try await assertOwnership?()
            do {
                try await client.delete(path: target.path)
                deletedCount += 1
                deletedBytes += target.size
            } catch {
                if RemoteFaultLite.classify(error) == .cancelled { throw error }
                failedCount += 1
            }
            onProgress?(index + 1, total)
        }

        return ThumbnailOrphanDeleteResult(
            deletedCount: deletedCount,
            deletedBytes: deletedBytes,
            failedCount: failedCount
        )
    }

    // Parses the fingerprint hex from a known sidecar name. Fail-closed: only a 64-char lowercase
    // hex stem (our exact `Data.hexString` naming) qualifies, so a stray non-thumbnail file under
    // thumbs/ is never treated as a deletable orphan.
    static func fingerprintHex(fromFileName name: String) -> String? {
        sidecarKey(fromFileName: name)?.fingerprintHex
    }

    static func sidecarKey(fromFileName name: String) -> RemoteThumbnailSidecarKey? {
        let suffixes: [(suffix: String, storageCodec: Int)] = [
            (".jpg", RemoteManifestResource.plaintextStorageCodec),
            (".\(RemoteFileNaming.encryptedFileExtension)", RemoteManifestResource.encryptedStorageCodec)
        ]
        guard let match = suffixes.first(where: { name.hasSuffix($0.suffix) }) else { return nil }
        let suffix = match.suffix
        let stem = String(name.dropLast(suffix.count))
        guard stem.count == 64,
              stem.allSatisfy({ ("0"..."9").contains($0) || ("a"..."f").contains($0) })
        else { return nil }
        return RemoteThumbnailSidecarKey(fingerprintHex: stem, storageCodec: match.storageCodec)
    }
}
