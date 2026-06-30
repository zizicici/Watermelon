import Foundation

// A content-addressed thumbnail sidecar (.watermelon/thumbs/<shard>/<fp>.jpg) whose fingerprint is
// absent from the authoritative live set (every month manifest's asset fingerprints).
struct ThumbnailOrphan: Sendable, Hashable {
    let fingerprintHex: String
    let path: String
    let size: Int64
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

// Garbage-collects orphan thumbnail sidecars. An orphan is a `<fp>.jpg` whose fingerprint is not in
// `liveFingerprintHexes` — the union of every month manifest's asset fingerprints. The live set MUST be
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
    let liveFingerprintHexes: Set<String>

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
                guard let hex = Self.fingerprintHex(fromFileName: file.name) else { continue }
                if !liveFingerprintHexes.contains(hex) {
                    orphans.append(ThumbnailOrphan(fingerprintHex: hex, path: file.path, size: file.size))
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
            if liveFingerprintHexes.contains(target.fingerprintHex) {
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

    // Parses the fingerprint hex from a `<hex>.jpg` sidecar name. Fail-closed: only a 64-char lowercase
    // hex stem (our exact `Data.hexString` naming) qualifies, so a stray non-thumbnail `.jpg` under
    // thumbs/ is never treated as a deletable orphan.
    static func fingerprintHex(fromFileName name: String) -> String? {
        let suffix = ".jpg"
        guard name.hasSuffix(suffix) else { return nil }
        let stem = String(name.dropLast(suffix.count))
        guard stem.count == 64,
              stem.allSatisfy({ ("0"..."9").contains($0) || ("a"..."f").contains($0) })
        else { return nil }
        return stem
    }
}
