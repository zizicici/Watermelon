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
        guard manifest.formatVersion == formatVersion,
              manifest.layout == layout,
              let minVersion = manifest.minAppVersion else { return false }
        return minAppVersion.compare(minVersion, options: .numeric) != .orderedAscending
    }
}

// Commits `version.json` crash-aware: uploads to a temp sibling, publishes by move, then reads the final
// back before reporting success, so a truncated/interrupted write never leaves a half-committed format
// marker at the canonical path that a later router would trust.
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
        // Temp sibling under `.watermelon`: a `.tmp` suffix that classify/readVersion never mistake for the
        // committed `version.json` (which is read by exact name).
        let tempPath = RepoLayoutLite.repoDirectoryPath(basePath: basePath)
            + "/version_\(UUID().uuidString).json.tmp"

        try await client.createDirectory(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))

        let uploadURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.versionFileName)
        defer { try? FileManager.default.removeItem(at: uploadURL) }
        try data.write(to: uploadURL)

        do {
            try await client.upload(
                localURL: uploadURL,
                remotePath: tempPath,
                respectTaskCancellation: false,
                onProgress: nil
            )
            try await publish(tempPath: tempPath, finalPath: versionPath)
        } catch {
            // Best-effort temp cleanup; never delete a valid final version.json that publish may have left.
            if (try? await client.exists(path: tempPath)) == true {
                try? await client.delete(path: tempPath)
            }
            throw error
        }

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

    // Move the uploaded temp onto the canonical path. A direct move atomically replaces on backends that
    // allow it; when a backend refuses to overwrite an existing (e.g. malformed) final, back the final up
    // first so the canonical path is never left absent without a recoverable copy present.
    private func publish(tempPath: String, finalPath: String) async throws {
        do {
            try await client.move(from: tempPath, to: finalPath)
            return
        } catch {
            guard (try? await client.exists(path: finalPath)) == true else { throw error }
            let backupPath = RepoLayoutLite.repoDirectoryPath(basePath: basePath)
                + "/version_\(UUID().uuidString).json.bak"
            try await client.move(from: finalPath, to: backupPath)
            do {
                try await client.move(from: tempPath, to: finalPath)
            } catch {
                try? await client.move(from: backupPath, to: finalPath)
                throw error
            }
            if (try? await client.exists(path: backupPath)) == true {
                try? await client.delete(path: backupPath)
            }
        }
    }
}
