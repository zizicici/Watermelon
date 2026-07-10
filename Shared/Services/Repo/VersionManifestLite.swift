import Foundation

// Repo Lite version manifest. `version.json` is the single format commit point; a repo is only
// "current" once this file is committed with a supported format.
nonisolated enum VersionManifestLite {
    // `formatVersion` is the compatibility boundary. Each min-app value is the first release that supports
    // that format, not the current release; ordinary app releases must not bump it.
    static let plainFormatVersion = 2
    static let encryptedFormatVersion = 3
    static let formatVersion = plainFormatVersion
    static let latestFormatVersion = encryptedFormatVersion
    static let plainMinAppVersion = "1.5.0"
    static let encryptedMinAppVersion = "1.6.0"
    static let minAppVersion = plainMinAppVersion
    static let latestMinAppVersion = encryptedMinAppVersion
    static let encryptionMode = "per-resource"
    static let contentCodec = "wmenc-aes256-gcm-chunked-v1"
    static let keyAlgorithm = "AES-256-GCM-HKDF-SHA256"

    enum Compatibility: Equatable, Sendable {
        case readableWritable
        case unsupported(minAppVersion: String?)
        case damaged
    }

    static func makeManifest(createdAt: String, createdBy: String) -> WatermelonRemoteVersionManifest {
        WatermelonRemoteVersionManifest(
            formatVersion: plainFormatVersion,
            minAppVersion: plainMinAppVersion,
            createdAt: createdAt,
            createdBy: createdBy
        )
    }

    static func makeEncryptedManifest(
        createdAt: String,
        createdBy: String,
        repoID: String,
        activeKeyID: String,
        keyCheck: String
    ) -> WatermelonRemoteVersionManifest {
        WatermelonRemoteVersionManifest(
            formatVersion: encryptedFormatVersion,
            minAppVersion: encryptedMinAppVersion,
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: repoID,
            encryption: WatermelonRemoteVersionManifest.Encryption(
                mode: encryptionMode,
                contentCodec: contentCodec,
                activeKeyID: activeKeyID,
                keys: [
                    WatermelonRemoteVersionManifest.Key(
                        kid: activeKeyID,
                        alg: keyAlgorithm,
                        status: "active",
                        createdAt: createdAt,
                        keyCheck: keyCheck
                    )
                ],
                manifestEncrypted: false,
                resourceMetadataEncrypted: true
            )
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

    static func compatibility(for data: Data) -> Compatibility {
        guard let manifest = try? decode(data) else { return .damaged }
        return compatibility(for: manifest)
    }

    static func compatibility(for manifest: WatermelonRemoteVersionManifest) -> Compatibility {
        guard let remoteFormat = manifest.formatVersion else { return .damaged }
        if remoteFormat > latestFormatVersion {
            return .unsupported(minAppVersion: unsupportedMinAppVersion(from: manifest))
        }
        guard remoteFormat == plainFormatVersion || remoteFormat == encryptedFormatVersion else {
            return .unsupported(minAppVersion: unsupportedMinAppVersion(from: manifest))
        }
        guard let remoteMinAppVersion = manifest.minAppVersion, !remoteMinAppVersion.isEmpty,
              let createdAt = manifest.createdAt, !createdAt.isEmpty,
              let createdBy = manifest.createdBy, !createdBy.isEmpty else {
            return .damaged
        }
        if remoteFormat == encryptedFormatVersion {
            return encryptedCompatibility(for: manifest)
        }
        guard manifest.repoID == nil, manifest.encryption == nil else { return .damaged }
        return .readableWritable
    }

    static func isCurrent(_ manifest: WatermelonRemoteVersionManifest) -> Bool {
        compatibility(for: manifest) == .readableWritable
    }

    static func isVersionScratchFileName(_ name: String) -> Bool {
        RepoLayoutLite.isVersionScratchFileName(name)
    }

    static func isVersionTempScratchFileName(_ name: String) -> Bool {
        RepoLayoutLite.isVersionTempScratchFileName(name)
    }

    static func isVersionBackupScratchFileName(_ name: String) -> Bool {
        RepoLayoutLite.isVersionBackupScratchFileName(name)
    }

    private static func unsupportedMinAppVersion(from manifest: WatermelonRemoteVersionManifest) -> String? {
        guard let remoteMinAppVersion = manifest.minAppVersion,
              latestMinAppVersion.compare(remoteMinAppVersion, options: .numeric) == .orderedAscending else {
            return nil
        }
        return remoteMinAppVersion
    }

    private static func encryptedCompatibility(for manifest: WatermelonRemoteVersionManifest) -> Compatibility {
        guard let repoID = manifest.repoID, !repoID.isEmpty,
              let encryption = manifest.encryption else {
            return .damaged
        }
        guard let mode = encryption.mode, !mode.isEmpty,
              let codec = encryption.contentCodec, !codec.isEmpty else {
            return .damaged
        }
        guard mode == encryptionMode, codec == contentCodec else {
            return .unsupported(minAppVersion: unsupportedMinAppVersion(from: manifest))
        }
        guard encryption.manifestEncrypted == false,
              encryption.resourceMetadataEncrypted == true,
              let activeKeyID = encryption.activeKeyID, !activeKeyID.isEmpty,
              let keys = encryption.keys, !keys.isEmpty,
              let activeKey = keys.first(where: { $0.kid == activeKeyID }) else {
            return .damaged
        }
        guard let keyAlgorithm = activeKey.alg, !keyAlgorithm.isEmpty,
              let status = activeKey.status, !status.isEmpty,
              let keyCreatedAt = activeKey.createdAt, !keyCreatedAt.isEmpty,
              let keyCheck = activeKey.keyCheck, !keyCheck.isEmpty else {
            return .damaged
        }
        guard keyAlgorithm == Self.keyAlgorithm, status == "active" else {
            return .unsupported(minAppVersion: unsupportedMinAppVersion(from: manifest))
        }
        return .readableWritable
    }
}

// Commits `version.json` crash-aware: uploads to a temp sibling, publishes by move, then reads the final
// back before reporting success, so a truncated/interrupted write never leaves a half-committed format
// marker at the canonical path that a later router would trust.
struct VersionManifestWriter: Sendable {
    enum WriteError: Error, Equatable {
        case readBackMismatch
        case unsafeExistingVersion
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
        return try await commit(manifest: manifest)
    }

    @discardableResult
    func commit(manifest: WatermelonRemoteVersionManifest) async throws -> WatermelonRemoteVersionManifest {
        guard VersionManifestLite.compatibility(for: manifest) == .readableWritable else {
            throw WriteError.unsafeExistingVersion
        }
        let data = try VersionManifestLite.encode(manifest)
        return try await commit(manifest: manifest, data: data)
    }

    @discardableResult
    func commit(versionData data: Data) async throws -> WatermelonRemoteVersionManifest {
        let manifest = try VersionManifestLite.decode(data)
        guard VersionManifestLite.compatibility(for: manifest) == .readableWritable else {
            throw WriteError.unsafeExistingVersion
        }
        return try await commit(manifest: manifest, data: data)
    }

    @discardableResult
    private func commit(
        manifest: WatermelonRemoteVersionManifest,
        data: Data
    ) async throws -> WatermelonRemoteVersionManifest {
        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)
        // Temp sibling under `.watermelon`: a `.tmp` suffix that classify/readVersion never mistake for the
        // committed `version.json` (which is read by exact name).
        let tempPath = RepoLayoutLite.versionTempPath(basePath: basePath)

        try await assertOwnedOrThrow()
        try await client.createDirectory(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
        try await assertCanonicalVersionSafeToReplace(
            versionPath,
            replacementFormatVersion: manifest.formatVersion
        )

        let uploadURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.versionFileName)
        defer { try? FileManager.default.removeItem(at: uploadURL) }
        try data.write(to: uploadURL)

        // Non-independent MOVE backend: overwrite version.json directly. temp→MOVE aliases the temp to the
        // canonical, so deleting that temp (here or in cleanup) would destroy version.json.
        if await client.resolveMoveIsNonIndependent(basePath: basePath) {
            try await commitByDirectPut(uploadURL: uploadURL, versionPath: versionPath, data: data)
            return manifest
        }

        do {
            try await client.upload(
                localURL: uploadURL,
                remotePath: tempPath,
                respectTaskCancellation: false,
                onProgress: nil
            )
            try await publish(tempPath: tempPath, finalPath: versionPath)
        } catch {
            // Keep the temp as recovery scratch when the canonical is absent and a backup scratch survives.
            if !(await keepTempAsRecoveryScratch(versionPath: versionPath)),
               (try? await client.exists(path: tempPath)) == true {
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
            // The published bytes don't match what we wrote: remove the damaged canonical so the repo routes
            // recoverable (.fresh/.v1Migrate/.malformedVersion) next run instead of terminal .damaged. This
            // cleanup is cancellation-shielded and retried — a swallowed/cancelled delete would leave the
            // proven-bad bytes as the only commit point, which the router treats as terminal .damaged.
            // version.json carries no user data, so a re-commit is harmless.
            await removeProvenBadCanonical(versionPath: versionPath)
            throw WriteError.readBackMismatch
        }
        return manifest
    }

    // Cancellation-shielded, bounded-retry removal of a canonical whose bytes were just proven wrong.
    // Re-proves ownership first so a lost lease leaves the canonical for a successor instead of deleting it.
    private func removeProvenBadCanonical(versionPath: String) async {
        await Task {
            for attempt in 0..<3 {
                // Re-prove ownership before every destructive attempt: a delete that applies remotely but
                // returns a retryable fault can outlive the lease (a successor may then commit a valid
                // version.json), and a stale writer must never delete a successor's canonical on retry.
                do { try await assertOwnedOrThrow() } catch { return }
                do {
                    try await client.delete(path: versionPath)
                    return
                } catch {
                    if RemoteFaultLite.classify(error) == .notFound { return }
                    if attempt == 2 { return }
                }
            }
        }.value
    }

    // Direct-PUT commit for non-independent MOVE backends: overwrite version.json, read it back byte-exact,
    // and remove a proven-bad canonical so the repo routes recoverable next run. version.json carries no user
    // data (recovery is a re-mint), and it is a ~100-byte one-shot PUT, so a partial-persist crash is effectively
    // impossible — a durable scratch would only route to a recovery that assertCanonicalVersionSafeToReplace refuses.
    private func commitByDirectPut(uploadURL: URL, versionPath: String, data: Data) async throws {
        try await assertOwnedOrThrow()
        do {
            try await client.upload(
                localURL: uploadURL,
                remotePath: versionPath,
                respectTaskCancellation: false,
                onProgress: nil
            )
        } catch {
            // A post-effect upload failure can leave a partial/corrupt version.json. Read it back and remove it
            // only if malformed — a valid manifest (ours-that-landed, or a prior commit) is a usable commit point,
            // an inconclusive read never deletes — so the repo re-mints instead of wedging terminal .damaged.
            await removeCanonicalIfMalformed(versionPath: versionPath)
            throw error
        }
        let readBackURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.versionFileName)
        defer { try? FileManager.default.removeItem(at: readBackURL) }
        try await client.download(remotePath: versionPath, localURL: readBackURL)
        guard let readBackData = try? Data(contentsOf: readBackURL), readBackData == data else {
            await removeProvenBadCanonical(versionPath: versionPath)
            throw WriteError.readBackMismatch
        }
    }

    // Removes the canonical only if it reads back as a malformed version manifest (a valid one — ours or a prior
    // commit — stays). Shielded and ownership-re-proving via removeProvenBadCanonical; inconclusive reads never delete.
    private func removeCanonicalIfMalformed(versionPath: String) async {
        await Task {
            let readURL = FileManager.default.temporaryDirectory
                .appendingPathComponent(UUID().uuidString)
                .appendingPathExtension(RepoLayoutLite.versionFileName)
            defer { try? FileManager.default.removeItem(at: readURL) }
            do {
                try await client.download(remotePath: versionPath, localURL: readURL)
            } catch {
                return
            }
            let bytes = (try? Data(contentsOf: readURL)) ?? Data()
            guard VersionManifestLite.compatibility(for: bytes) != .readableWritable else { return }
            await removeProvenBadCanonical(versionPath: versionPath)
        }.value
    }

    // Move the uploaded temp onto the canonical path. A direct move atomically replaces on backends that
    // allow it; when a backend refuses to overwrite an existing (e.g. malformed) final, back the final up
    // first so the canonical path is never left absent without a recoverable copy present.
    private func publish(tempPath: String, finalPath: String) async throws {
        let backupPath = RepoLayoutLite.versionBackupPath(basePath: basePath)
        try await RemoteMoveReplace.moveReplacing(
            client: client,
            tempPath: tempPath,
            finalPath: finalPath,
            backupPath: backupPath,
            ignoreCancellation: false,
            assertOwnership: { try await assertOwnedOrThrow() }
        )
    }

    private func keepTempAsRecoveryScratch(versionPath: String) async -> Bool {
        if (try? await client.exists(path: versionPath)) == true { return false }
        // A LIST fault must not read as "no backup scratch": that would license deleting the only current-version copy and route a recoverable repo terminal .damaged.
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
        } catch {
            return true
        }
        return entries.contains { VersionManifestLite.isVersionBackupScratchFileName($0.name) }
    }

    private func assertOwnedOrThrow() async throws {
        try await assertOwnership?()
    }

    private func assertCanonicalVersionSafeToReplace(
        _ versionPath: String,
        replacementFormatVersion: Int?
    ) async throws {
        let readURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.versionFileName)
        defer { try? FileManager.default.removeItem(at: readURL) }

        do {
            try await client.download(remotePath: versionPath, localURL: readURL)
        } catch {
            if RemoteFaultLite.classify(error) == .notFound {
                return
            }
            throw error
        }

        guard let data = try? Data(contentsOf: readURL),
              let existingManifest = try? VersionManifestLite.decode(data),
              VersionManifestLite.compatibility(for: existingManifest) == .readableWritable,
              let existingFormatVersion = existingManifest.formatVersion,
              let replacementFormatVersion,
              existingFormatVersion <= replacementFormatVersion else {
            throw WriteError.unsafeExistingVersion
        }
    }
}

nonisolated enum RemoteMoveReplace {
    // Returns whether a prior existing final was backed up to `backupPath` (the backup-first path ran). A fresh
    // direct publish — no prior canonical — returns false; only an existing-final overwrite returns true. The
    // caller uses this to decide proven-bad-canonical recovery: a fresh publish may be deleted on a bad read-back,
    // an existing-canonical overwrite must be reverted to its backup instead.
    @discardableResult
    static func moveReplacing(
        client: any RemoteStorageClientProtocol,
        tempPath: String,
        finalPath: String,
        backupPath: String,
        ignoreCancellation: Bool,
        assertOwnership: @escaping @Sendable () async throws -> Void,
        backupExistingFinal: Bool = false,
        onRenameFailure: ((Error) -> Void)? = nil
    ) async throws -> Bool {
        try checkCancellation(unless: ignoreCancellation)
        try await shielded(ignoreCancellation) { try await assertOwnership() }

        // Callers that validate the replacement only after this returns (Lite month manifests) must not let a
        // direct overwrite replace an existing final with no retained backup: on an overwrite-permitting
        // backend that would lose the prior good copy if the caller's read-back later fails. Probe up front
        // and route an existing final through the backup-first path below; an unresolved probe assumes an
        // existing final (fail-safe). The destructive backup move re-proves ownership before it runs.
        var existingFinalNeedsBackup = false
        var probedFinalAbsent = false
        if backupExistingFinal {
            do {
                existingFinalNeedsBackup = try await shielded(ignoreCancellation) {
                    try await client.exists(path: finalPath)
                }
                probedFinalAbsent = !existingFinalNeedsBackup
            } catch {
                if !ignoreCancellation, Task.isCancelled || error is CancellationError {
                    throw CancellationError()
                }
                existingFinalNeedsBackup = true
            }
        }

        if !existingFinalNeedsBackup {
            // The final-existence probe above (only taken when `backupExistingFinal`) is an awaited round-trip
            // during which the lease can lapse; re-prove ownership before the direct publish so a stale writer
            // can't overwrite a successor's freshly published canonical. The backup-first, not-found-fallback,
            // and restore branches already re-prove after their awaited probes — this closes the same gap for
            // the fresh direct-publish branch. (No probe ran when `backupExistingFinal` is false, so the
            // assertion above already immediately precedes the move and no extra re-proof is needed.)
            if backupExistingFinal {
                try checkCancellation(unless: ignoreCancellation)
                try await shielded(ignoreCancellation) { try await assertOwnership() }
            }
            do {
                try await shielded(ignoreCancellation) {
                    try await client.move(from: tempPath, to: finalPath)
                }
                return false   // fresh direct publish: no prior canonical was backed up
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
                // Copy+delete backends (S3) can publish `finalPath` while the temp-source delete faults: when
                // the probe proved no prior final, this is our own fresh publish, not a prior canonical, so the
                // backup-first path would back up our own bytes and falsely report a prior-canonical backup.
                if probedFinalAbsent {
                    try? await shielded(ignoreCancellation) { try await client.delete(path: tempPath) }
                    return false
                }
            }
        }

        try checkCancellation(unless: ignoreCancellation)
        try await shielded(ignoreCancellation) { try await assertOwnership() }
        do {
            try await shielded(ignoreCancellation) {
                try await client.move(from: finalPath, to: backupPath)
            }
        } catch {
            // The fail-safe (or a fallback after a refused direct overwrite) assumed an existing final, but
            // the backup move proves it is absent: there is nothing to back up (e.g. a fresh canonical whose
            // existence probe transiently faulted), so publish directly instead of aborting the flush.
            if RemoteFaultLite.classify(error) == .notFound {
                try checkCancellation(unless: ignoreCancellation)
                try await shielded(ignoreCancellation) { try await assertOwnership() }
                try await shielded(ignoreCancellation) {
                    try await client.move(from: tempPath, to: finalPath)
                }
                return false   // no prior final existed (probe was a fail-safe): nothing was backed up
            }
            await restoreBackupIfFinalMissing(
                client: client,
                backupPath: backupPath,
                finalPath: finalPath,
                assertOwnership: assertOwnership
            )
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
            // Restore only while the final is absent — never clobbers a foreign final, always self-heals.
            if !ignoreCancellation, Task.isCancelled || error is CancellationError {
                await restoreBackupIfFinalMissing(
                    client: client,
                    backupPath: backupPath,
                    finalPath: finalPath,
                    assertOwnership: assertOwnership
                )
                try await shielded(ignoreCancellation) { try await assertOwnership() }
                throw CancellationError()
            }
            await restoreBackupIfFinalMissing(
                client: client,
                backupPath: backupPath,
                finalPath: finalPath,
                assertOwnership: assertOwnership
            )
            try await shielded(ignoreCancellation) { try await assertOwnership() }
            onRenameFailure?(error)
            throw error
        }

        // Do not delete the backup here: the caller validates the replacement (version read-back /
        // verifyRemoteManifestBytes) only after this returns, so deleting the prior good final now would
        // destroy the sole recovery copy when that read-back fails. OrphanCleanupLite reclaims a redundant
        // backup once the canonical validates and restores it over an invalid canonical, so the retained
        // backup is recovery scratch, not a leak.
        return true   // an existing prior canonical was backed up to `backupPath` before the overwrite
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
        finalPath: String,
        assertOwnership: @escaping @Sendable () async throws -> Void
    ) async {
        await Task {
            // An unresolved probe could mean a successor committed.
            let present: Bool
            do { present = try await client.exists(path: finalPath) } catch { return }
            guard !present else { return }
            // Re-prove after the await to avoid moving over a successor final.
            do { try await assertOwnership() } catch { return }
            try? await client.move(from: backupPath, to: finalPath)
        }.value
    }
}
