import Foundation

// Dormant Repo V2 version manifest (Stage B, Step 4). `version.json` is the single format commit
// point; a repo is only "current" once this file is committed with format 2 + the lite layout.
// Nothing in production writes it yet — this just pins the canonical schema and a read-back-verified
// writer so later cutover work shares one source of truth. Reuses WatermelonRemoteVersionManifest as
// the on-the-wire model rather than introducing a competing one.
nonisolated enum VersionManifestLite {
    static let formatVersion = 2
    static let layout = "lite-month-sqlite"
    static let minAppVersion = "1.5.0"

    static func makeManifest(createdAt: String, createdBy: String) -> WatermelonRemoteVersionManifest {
        WatermelonRemoteVersionManifest(
            formatVersion: formatVersion,
            layout: layout,
            minAppVersion: minAppVersion,
            createdAt: createdAt,
            createdBy: createdBy
        )
    }

    static func encode(_ manifest: WatermelonRemoteVersionManifest) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        return try encoder.encode(manifest)
    }

    static func decode(_ data: Data) throws -> WatermelonRemoteVersionManifest {
        try JSONDecoder().decode(WatermelonRemoteVersionManifest.self, from: data)
    }

    static func isCurrent(_ manifest: WatermelonRemoteVersionManifest) -> Bool {
        manifest.formatVersion == formatVersion && manifest.layout == layout
    }
}

// Commits `version.json` and reads it back before reporting success, so a truncated or rejected write
// never leaves a half-committed format marker that a later router would trust.
struct VersionManifestWriter: Sendable {
    enum WriteError: Error, Equatable {
        case readBackMismatch
    }

    let client: any RemoteStorageClientProtocol
    let basePath: String

    @discardableResult
    func commit(createdAt: String, createdBy: String) async throws -> WatermelonRemoteVersionManifest {
        let manifest = VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        let data = try VersionManifestLite.encode(manifest)
        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)

        try await client.createDirectory(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))

        let uploadURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.versionFileName)
        defer { try? FileManager.default.removeItem(at: uploadURL) }
        try data.write(to: uploadURL)
        try await client.upload(
            localURL: uploadURL,
            remotePath: versionPath,
            respectTaskCancellation: false,
            onProgress: nil
        )

        let readBackURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.versionFileName)
        defer { try? FileManager.default.removeItem(at: readBackURL) }
        try await client.download(remotePath: versionPath, localURL: readBackURL)
        // Byte-exact, not decode-equal: divergent key order / whitespace / ignored extra fields must
        // not pass — version.json is the only format commit point.
        let readBackData = try Data(contentsOf: readBackURL)
        guard readBackData == data else {
            throw WriteError.readBackMismatch
        }
        return manifest
    }
}
