import Foundation

enum RepoEncryptionSetupError: Error, Equatable {
    case damagedVersion
    case unsupportedVersion(minAppVersion: String?)
    case missingEncryptedRepo
    case missingLocalKey
    case keyVerificationFailed
}

extension RepoEncryptionSetupError: LocalizedError {
    var errorDescription: String? {
        switch self {
        case .damagedVersion:
            return String(localized: "repo.encryption.error.damagedVersion", defaultValue: "The repository encryption metadata is damaged.")
        case .unsupportedVersion(let minAppVersion):
            if let minAppVersion, !minAppVersion.isEmpty {
                return String(
                    format: String(
                        localized: "repo.encryption.error.unsupportedVersion.withMinimum",
                        defaultValue: "This encrypted repository requires Watermelon %@ or later."
                    ),
                    minAppVersion
                )
            }
            return String(localized: "repo.encryption.error.unsupportedVersion", defaultValue: "This encrypted repository requires a newer version of Watermelon.")
        case .missingEncryptedRepo:
            return String(localized: "repo.encryption.error.missingEncryptedRepo", defaultValue: "This repository is not encrypted yet.")
        case .missingLocalKey:
            return String(localized: "repo.encryption.error.missingLocalKey", defaultValue: "This device is missing the repository encryption key. Import the recovery key.")
        case .keyVerificationFailed:
            return String(localized: "repo.encryption.error.keyVerificationFailed", defaultValue: "The recovery key does not match this encrypted repository.")
        }
    }
}

struct RepoEncryptionSetupResult: Equatable, Sendable {
    enum Action: Equatable, Sendable {
        case createdEncryptedRepo
        case upgradedPlainRepo
        case verifiedExistingEncryptedRepo
        case importedRecoveryKey
    }

    let action: Action
    let manifest: WatermelonRemoteVersionManifest
    let keyMaterial: RepoEncryptionKeyMaterial
    let recoveryKey: String
    let context: RepoEncryptionContext
}

struct RepoEncryptionSetupService: Sendable {
    let keyStore: any RepoEncryptionKeyStore
    let makeKeyMaterial: @Sendable () throws -> RepoEncryptionKeyMaterial

    init(
        keyStore: any RepoEncryptionKeyStore,
        makeKeyMaterial: @escaping @Sendable () throws -> RepoEncryptionKeyMaterial = {
            try RepoEncryptionKeyCodec.generate(
                repoID: UUID().uuidString.lowercased(),
                keyID: UUID().uuidString.lowercased()
            )
        }
    ) {
        self.keyStore = keyStore
        self.makeKeyMaterial = makeKeyMaterial
    }

    func enableEncryption(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        createdAt: String,
        createdBy: String,
        assertOwnership: MonthManifestOwnershipAssertion? = nil
    ) async throws -> RepoEncryptionSetupResult {
        let existing = try await readCommittedManifestIfPresent(client: client, basePath: basePath)
        guard let existing else {
            return try await createEncryptedManifest(
                action: .createdEncryptedRepo,
                client: client,
                basePath: basePath,
                createdAt: createdAt,
                createdBy: createdBy,
                assertOwnership: assertOwnership
            )
        }

        switch VersionManifestLite.compatibility(for: existing) {
        case .readableWritable:
            switch existing.formatVersion {
            case VersionManifestLite.plainFormatVersion:
                return try await createEncryptedManifest(
                    action: .upgradedPlainRepo,
                    client: client,
                    basePath: basePath,
                    createdAt: createdAt,
                    createdBy: createdBy,
                    assertOwnership: assertOwnership
                )
            case VersionManifestLite.encryptedFormatVersion:
                return try verifyExistingEncryptedManifest(existing)
            default:
                throw RepoEncryptionSetupError.damagedVersion
            }
        case .damaged:
            throw RepoEncryptionSetupError.damagedVersion
        case .unsupported(let minAppVersion):
            throw RepoEncryptionSetupError.unsupportedVersion(minAppVersion: minAppVersion)
        }
    }

    func loadExistingContext(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> RepoEncryptionContext {
        guard let manifest = try await readCommittedManifestIfPresent(client: client, basePath: basePath) else {
            throw RepoEncryptionSetupError.missingEncryptedRepo
        }
        switch VersionManifestLite.compatibility(for: manifest) {
        case .readableWritable:
            guard manifest.formatVersion == VersionManifestLite.encryptedFormatVersion else {
                throw RepoEncryptionSetupError.missingEncryptedRepo
            }
            return try verifyExistingEncryptedManifest(manifest).context
        case .damaged:
            throw RepoEncryptionSetupError.damagedVersion
        case .unsupported(let minAppVersion):
            throw RepoEncryptionSetupError.unsupportedVersion(minAppVersion: minAppVersion)
        }
    }

    func verifyExistingEncryptedRepo(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> RepoEncryptionSetupResult {
        guard let manifest = try await readCommittedManifestIfPresent(client: client, basePath: basePath) else {
            throw RepoEncryptionSetupError.missingEncryptedRepo
        }
        switch VersionManifestLite.compatibility(for: manifest) {
        case .readableWritable:
            guard manifest.formatVersion == VersionManifestLite.encryptedFormatVersion else {
                throw RepoEncryptionSetupError.missingEncryptedRepo
            }
            return try verifyExistingEncryptedManifest(manifest)
        case .damaged:
            throw RepoEncryptionSetupError.damagedVersion
        case .unsupported(let minAppVersion):
            throw RepoEncryptionSetupError.unsupportedVersion(minAppVersion: minAppVersion)
        }
    }

    func importRecoveryKey(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        recoveryKey: String
    ) async throws -> RepoEncryptionSetupResult {
        let material = try RepoEncryptionKeyCodec.decodeRecoveryKey(recoveryKey)
        guard let manifest = try await readCommittedManifestIfPresent(client: client, basePath: basePath) else {
            throw RepoEncryptionSetupError.missingEncryptedRepo
        }
        switch VersionManifestLite.compatibility(for: manifest) {
        case .readableWritable:
            guard manifest.formatVersion == VersionManifestLite.encryptedFormatVersion,
                  manifest.repoID == material.repoID,
                  manifest.encryption?.activeKeyID == material.keyID else {
                throw RepoEncryptionSetupError.keyVerificationFailed
            }
            let context: RepoEncryptionContext
            do {
                context = try RepoEncryptionContext.verified(manifest: manifest, keyData: material.keyData)
            } catch {
                throw RepoEncryptionSetupError.keyVerificationFailed
            }
            try keyStore.save(material)
            return RepoEncryptionSetupResult(
                action: .importedRecoveryKey,
                manifest: manifest,
                keyMaterial: material,
                recoveryKey: RepoEncryptionKeyCodec.recoveryKeyString(for: material),
                context: context
            )
        case .damaged:
            throw RepoEncryptionSetupError.damagedVersion
        case .unsupported(let minAppVersion):
            throw RepoEncryptionSetupError.unsupportedVersion(minAppVersion: minAppVersion)
        }
    }

    private func createEncryptedManifest(
        action: RepoEncryptionSetupResult.Action,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        createdAt: String,
        createdBy: String,
        assertOwnership: MonthManifestOwnershipAssertion?
    ) async throws -> RepoEncryptionSetupResult {
        let material = try makeKeyMaterial()
        let keyCheck = try RepoEncryptionKeyCodec.keyCheck(
            repoID: material.repoID,
            keyID: material.keyID,
            keyData: material.keyData
        )
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: createdAt,
            createdBy: createdBy,
            repoID: material.repoID,
            activeKeyID: material.keyID,
            keyCheck: keyCheck
        )

        try keyStore.save(material)
        do {
            let committed = try await VersionManifestWriter(
                client: client,
                basePath: basePath,
                assertOwnership: assertOwnership
            ).commit(manifest: manifest)
            return try setupResult(action: action, manifest: committed, material: material)
        } catch {
            if let committed = try? await readCommittedManifestIfPresent(client: client, basePath: basePath),
               committed.repoID == material.repoID,
               committed.encryption?.activeKeyID == material.keyID,
               let result = try? setupResult(action: action, manifest: committed, material: material) {
                return result
            }
            // Ambiguous publish failures may have landed a v3 repo; keep the only local key for retry/recovery.
            throw error
        }
    }

    private func setupResult(
        action: RepoEncryptionSetupResult.Action,
        manifest: WatermelonRemoteVersionManifest,
        material: RepoEncryptionKeyMaterial
    ) throws -> RepoEncryptionSetupResult {
        let context = try RepoEncryptionContext.verified(manifest: manifest, keyData: material.keyData)
        return RepoEncryptionSetupResult(
            action: action,
            manifest: manifest,
            keyMaterial: material,
            recoveryKey: RepoEncryptionKeyCodec.recoveryKeyString(for: material),
            context: context
        )
    }

    private func verifyExistingEncryptedManifest(
        _ manifest: WatermelonRemoteVersionManifest
    ) throws -> RepoEncryptionSetupResult {
        guard let repoID = manifest.repoID,
              let activeKeyID = manifest.encryption?.activeKeyID else {
            throw RepoEncryptionSetupError.damagedVersion
        }
        let material: RepoEncryptionKeyMaterial
        do {
            material = try keyStore.read(repoID: repoID, keyID: activeKeyID)
        } catch {
            throw RepoEncryptionSetupError.missingLocalKey
        }
        do {
            let context = try RepoEncryptionContext.verified(manifest: manifest, keyData: material.keyData)
            return RepoEncryptionSetupResult(
                action: .verifiedExistingEncryptedRepo,
                manifest: manifest,
                keyMaterial: material,
                recoveryKey: RepoEncryptionKeyCodec.recoveryKeyString(for: material),
                context: context
            )
        } catch {
            throw RepoEncryptionSetupError.keyVerificationFailed
        }
    }

    private func readCommittedManifestIfPresent(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> WatermelonRemoteVersionManifest? {
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.versionFileName)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await client.download(remotePath: RepoLayoutLite.versionPath(basePath: basePath), localURL: localURL)
        } catch {
            if RemoteFaultLite.classify(error) == .notFound {
                return nil
            }
            throw error
        }

        guard let data = try? Data(contentsOf: localURL),
              let manifest = try? VersionManifestLite.decode(data) else {
            throw RepoEncryptionSetupError.damagedVersion
        }
        return manifest
    }
}

nonisolated enum RepoEncryptionWriteGate {
    static func validate(
        profile: ServerProfileRecord,
        probe: RepoFormatProbe,
        client: any RemoteStorageClientProtocol,
        keyStore: any RepoEncryptionKeyStore = RepoEncryptionKeychainStore(keychain: KeychainService())
    ) async throws {
        if profile.defaultResourceStorageIsEncrypted {
            try await validateEncryptedProfile(profile: profile, probe: probe, client: client, keyStore: keyStore)
        } else {
            try await validatePlaintextProfile(profile: profile, probe: probe, client: client)
        }
    }

    static func committedManifestIsEncrypted(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Bool {
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.versionFileName)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await client.download(remotePath: RepoLayoutLite.versionPath(basePath: basePath), localURL: localURL)
        } catch {
            if RemoteFaultLite.classify(error) == .notFound { return false }
            throw error
        }

        guard let data = try? Data(contentsOf: localURL),
              let manifest = try? VersionManifestLite.decode(data) else {
            throw RepoEncryptionSetupError.damagedVersion
        }
        switch VersionManifestLite.compatibility(for: manifest) {
        case .readableWritable:
            return manifest.formatVersion == VersionManifestLite.encryptedFormatVersion
        case .damaged:
            throw RepoEncryptionSetupError.damagedVersion
        case .unsupported(let minAppVersion):
            throw RepoEncryptionSetupError.unsupportedVersion(minAppVersion: minAppVersion)
        }
    }

    private static func validatePlaintextProfile(
        profile: ServerProfileRecord,
        probe: RepoFormatProbe,
        client: any RemoteStorageClientProtocol
    ) async throws {
        switch probe.decision {
        case .current:
            if try await committedManifestIsEncrypted(client: client, basePath: profile.basePath) {
                throw BackupError.resourceEncryptionNotConfirmed
            }
        case .malformedVersion:
            guard let data = probe.recoverableVersionData,
                  let manifest = try? VersionManifestLite.decode(data),
                  VersionManifestLite.compatibility(for: manifest) == .readableWritable,
                  manifest.formatVersion == VersionManifestLite.encryptedFormatVersion else {
                return
            }
            throw BackupError.resourceEncryptionNotConfirmed
        case .fresh, .v1Migrate, .damaged, .unsupported:
            return
        }
    }

    private static func validateEncryptedProfile(
        profile: ServerProfileRecord,
        probe: RepoFormatProbe,
        client: any RemoteStorageClientProtocol,
        keyStore: any RepoEncryptionKeyStore
    ) async throws {
        switch probe.decision {
        case .current:
            _ = try await RepoEncryptionSetupService(keyStore: keyStore).loadExistingContext(
                client: client,
                basePath: profile.basePath
            )
        case .malformedVersion:
            guard let data = probe.recoverableVersionData,
                  let manifest = try? VersionManifestLite.decode(data) else {
                throw RepoEncryptionSetupError.damagedVersion
            }
            _ = try verifyEncryptedManifest(manifest, keyStore: keyStore)
        case .fresh, .v1Migrate:
            throw RepoEncryptionSetupError.missingEncryptedRepo
        case .damaged:
            throw RepoEncryptionSetupError.damagedVersion
        case .unsupported(let minAppVersion):
            throw RepoEncryptionSetupError.unsupportedVersion(minAppVersion: minAppVersion)
        }
    }

    private static func verifyEncryptedManifest(
        _ manifest: WatermelonRemoteVersionManifest,
        keyStore: any RepoEncryptionKeyStore
    ) throws -> RepoEncryptionContext {
        switch VersionManifestLite.compatibility(for: manifest) {
        case .readableWritable:
            guard manifest.formatVersion == VersionManifestLite.encryptedFormatVersion,
                  let repoID = manifest.repoID,
                  let activeKeyID = manifest.encryption?.activeKeyID else {
                throw RepoEncryptionSetupError.missingEncryptedRepo
            }
            let material: RepoEncryptionKeyMaterial
            do {
                material = try keyStore.read(repoID: repoID, keyID: activeKeyID)
            } catch {
                throw RepoEncryptionSetupError.missingLocalKey
            }
            do {
                return try RepoEncryptionContext.verified(manifest: manifest, keyData: material.keyData)
            } catch {
                throw RepoEncryptionSetupError.keyVerificationFailed
            }
        case .damaged:
            throw RepoEncryptionSetupError.damagedVersion
        case .unsupported(let minAppVersion):
            throw RepoEncryptionSetupError.unsupportedVersion(minAppVersion: minAppVersion)
        }
    }
}
