import Foundation

nonisolated struct RemoteThumbnailSidecarKey: Hashable, Sendable {
    let fingerprintHex: String
    let storageCodec: Int
}

// Content-addressed thumbnail sidecar layout, shared by the backup writer and the remote browser.
// One file per unique asset content (key = assetFingerprint hex), sharded by the fingerprint's first
// byte so no directory grows unbounded and the same image never duplicates across months. Lives under
// `.watermelon/` so OrphanCleanupLite (which only touches `months/*.tmp` + `locks/`) leaves it alone.
nonisolated enum RemoteThumbnailPaths {
    static let directoryName = ".watermelon/thumbs"

    static func shard(forFingerprintHex fingerprintHex: String) -> String {
        let prefix = fingerprintHex.prefix(2)
        return prefix.isEmpty ? "_" : String(prefix)
    }

    static func fileName(fingerprintHex: String, storageCodec: Int = RemoteManifestResource.plaintextStorageCodec) -> String {
        if storageCodec == RemoteManifestResource.encryptedStorageCodec {
            return "\(fingerprintHex).\(RemoteFileNaming.encryptedFileExtension)"
        }
        return "\(fingerprintHex).jpg"
    }

    static func relativePath(
        fingerprintHex: String,
        storageCodec: Int = RemoteManifestResource.plaintextStorageCodec
    ) -> String {
        "\(directoryName)/\(shard(forFingerprintHex: fingerprintHex))/\(fileName(fingerprintHex: fingerprintHex, storageCodec: storageCodec))"
    }

    static func shardDirectoryRelativePath(fingerprintHex: String) -> String {
        "\(directoryName)/\(shard(forFingerprintHex: fingerprintHex))"
    }

    static func absolutePath(
        basePath: String,
        fingerprintHex: String,
        storageCodec: Int = RemoteManifestResource.plaintextStorageCodec
    ) -> String {
        RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: relativePath(fingerprintHex: fingerprintHex, storageCodec: storageCodec)
        )
    }

    static func shardDirectoryAbsolutePath(basePath: String, fingerprintHex: String) -> String {
        RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: shardDirectoryRelativePath(fingerprintHex: fingerprintHex)
        )
    }

    static func rootAbsolutePath(basePath: String) -> String {
        RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: directoryName)
    }
}
