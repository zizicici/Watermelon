import CryptoKit
import Foundation

/// Repo-identity dedup key: SHA-256 of sorted `role|slot|hashHex` tokens
/// joined by `\n` (see `BackupAssetResourcePlanner.assetFingerprint`).
/// 32 bytes is enforced at every construction site.
struct AssetFingerprint: Hashable, Sendable, CustomStringConvertible {
    let rawValue: Data

    init?(decoding data: Data) {
        guard data.count == 32 else { return nil }
        self.rawValue = data
    }

    init(_ digest: SHA256Digest) {
        self.rawValue = Data(digest)
    }

    var description: String { rawValue.hexString }
}
