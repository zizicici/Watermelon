import Foundation

// Repo V2 (Lite) version manifest. `version.json` is the single format commit point; a repo is only
// "current" once this file is committed with format 2 + the lite layout.
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

    static func isVersionScratchFileName(_ name: String) -> Bool {
        isVersionTempScratchFileName(name) || isVersionBackupScratchFileName(name)
    }

    static func isVersionTempScratchFileName(_ name: String) -> Bool {
        hasValidToken(name, before: ".json.tmp")
    }

    static func isVersionBackupScratchFileName(_ name: String) -> Bool {
        hasValidToken(name, before: ".json.bak")
    }

    private static func hasValidToken(_ name: String, before suffix: String) -> Bool {
        guard name.hasPrefix("version_"), name.hasSuffix(suffix) else { return false }
        let tokenStart = name.index(name.startIndex, offsetBy: "version_".count)
        let tokenEnd = name.index(name.endIndex, offsetBy: -suffix.count)
        guard tokenStart < tokenEnd else { return false }
        return UUID(uuidString: String(name[tokenStart..<tokenEnd])) != nil
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
    let assertOwnership: MonthManifestOwnershipAssertion?

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        assertOwnership: MonthManifestOwnershipAssertion? = nil
    ) {
        self.client = client
        self.basePath = basePath
        self.assertOwnership = assertOwnership
    }

    @discardableResult
    func commit(createdAt: String, createdBy: String) async throws -> WatermelonRemoteVersionManifest {
        let manifest = VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        let data = try VersionManifestLite.encode(manifest)
        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)
        // Temp sibling under `.watermelon`: a `.tmp` suffix that classify/readVersion never mistake for the
        // committed `version.json` (which is read by exact name).
        let tempPath = RepoLayoutLite.repoDirectoryPath(basePath: basePath)
            + "/version_\(UUID().uuidString).json.tmp"

        try await assertOwnedOrThrow()
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
        let backupPath = RepoLayoutLite.repoDirectoryPath(basePath: basePath)
            + "/version_\(UUID().uuidString).json.bak"
        try await RemoteMoveReplace.moveReplacing(
            client: client,
            tempPath: tempPath,
            finalPath: finalPath,
            backupPath: backupPath,
            ignoreCancellation: false,
            assertOwnership: { try await assertOwnedOrThrow() }
        )
    }

    private func assertOwnedOrThrow() async throws {
        try await assertOwnership?()
    }
}

nonisolated enum RemoteMoveReplace {
    static func moveReplacing(
        client: any RemoteStorageClientProtocol,
        tempPath: String,
        finalPath: String,
        backupPath: String,
        ignoreCancellation: Bool,
        assertOwnership: @escaping @Sendable () async throws -> Void,
        onRenameFailure: ((Error) -> Void)? = nil
    ) async throws {
        try checkCancellation(unless: ignoreCancellation)
        try await shielded(ignoreCancellation) { try await assertOwnership() }

        do {
            try await shielded(ignoreCancellation) {
                try await client.move(from: tempPath, to: finalPath)
            }
            return
        } catch {
            if !ignoreCancellation, Task.isCancelled || error is CancellationError {
                throw CancellationError()
            }
            try checkCancellation(unless: ignoreCancellation)
            let finalExists = try await shielded(ignoreCancellation) {
                try await client.exists(path: finalPath)
            }
            guard finalExists else {
                onRenameFailure?(error)
                throw error
            }
        }

        try checkCancellation(unless: ignoreCancellation)
        try await shielded(ignoreCancellation) { try await assertOwnership() }
        do {
            try await shielded(ignoreCancellation) {
                try await client.move(from: finalPath, to: backupPath)
            }
        } catch {
            await restoreBackupIfFinalMissing(client: client, backupPath: backupPath, finalPath: finalPath)
            try await shielded(ignoreCancellation) { try await assertOwnership() }
            throw error
        }

        do {
            try checkCancellation(unless: ignoreCancellation)
            try await shielded(ignoreCancellation) { try await assertOwnership() }
            try await shielded(ignoreCancellation) {
                try await client.move(from: tempPath, to: finalPath)
            }
        } catch {
            if !ignoreCancellation, Task.isCancelled || error is CancellationError {
                await restoreBackupReplacingFinal(client: client, backupPath: backupPath, finalPath: finalPath)
                try await shielded(ignoreCancellation) { try await assertOwnership() }
                throw CancellationError()
            }
            await restoreBackupReplacingFinal(client: client, backupPath: backupPath, finalPath: finalPath)
            try await shielded(ignoreCancellation) { try await assertOwnership() }
            onRenameFailure?(error)
            throw error
        }

        try checkCancellation(unless: ignoreCancellation)
        if (try? await shielded(ignoreCancellation, { try await client.exists(path: backupPath) })) == true {
            try await shielded(ignoreCancellation) { try await assertOwnership() }
            try? await shielded(ignoreCancellation) { try await client.delete(path: backupPath) }
        }
    }

    private static func checkCancellation(unless ignoreCancellation: Bool) throws {
        if !ignoreCancellation {
            try Task.checkCancellation()
        }
    }

    private static func shielded<T: Sendable>(
        _ ignoreCancellation: Bool,
        _ operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        if ignoreCancellation {
            return try await Task { try await operation() }.value
        }
        return try await operation()
    }

    private static func restoreBackupIfFinalMissing(
        client: any RemoteStorageClientProtocol,
        backupPath: String,
        finalPath: String
    ) async {
        await Task {
            if (try? await client.exists(path: finalPath)) == true { return }
            try? await client.move(from: backupPath, to: finalPath)
        }.value
    }

    private static func restoreBackupReplacingFinal(
        client: any RemoteStorageClientProtocol,
        backupPath: String,
        finalPath: String
    ) async {
        await Task {
            guard (try? await client.exists(path: backupPath)) == true else { return }
            try? await client.delete(path: finalPath)
            try? await client.move(from: backupPath, to: finalPath)
        }.value
    }
}
