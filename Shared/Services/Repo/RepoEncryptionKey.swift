import CryptoKit
import Foundation
import Security

enum RepoEncryptionKeyError: Error, Equatable {
    case invalidKeyLength
    case invalidRecoveryKey
    case missingEncryptionConfig
    case missingActiveKey
    case keyCheckMismatch
    case randomGenerationFailed
}

struct RepoEncryptionKeyMaterial: Equatable, Sendable {
    static let byteCount = 32

    let repoID: String
    let keyID: String
    let keyData: Data

    init(repoID: String, keyID: String, keyData: Data) throws {
        guard keyData.count == Self.byteCount else { throw RepoEncryptionKeyError.invalidKeyLength }
        self.repoID = repoID
        self.keyID = keyID
        self.keyData = keyData
    }
}

nonisolated enum RepoEncryptionKeyCodec {
    private static let recoveryKeyPrefix = "WMENC1"
    private static let keyCheckDomain = "Watermelon repo key check v1"

    static func generate(repoID: String = UUID().uuidString, keyID: String = UUID().uuidString) throws -> RepoEncryptionKeyMaterial {
        try RepoEncryptionKeyMaterial(repoID: repoID, keyID: keyID, keyData: randomBytes(count: RepoEncryptionKeyMaterial.byteCount))
    }

    static func recoveryKeyString(for material: RepoEncryptionKeyMaterial) -> String {
        [
            recoveryKeyPrefix,
            base64URL(Data(material.repoID.utf8)),
            base64URL(Data(material.keyID.utf8)),
            base64URL(material.keyData)
        ].joined(separator: ".")
    }

    static func decodeRecoveryKey(_ text: String) throws -> RepoEncryptionKeyMaterial {
        let normalized = text.filter { !$0.isWhitespace }
        let parts = normalized.split(separator: ".", omittingEmptySubsequences: false)
        guard parts.count == 4, parts[0] == recoveryKeyPrefix,
              let repoIDData = base64URLDecode(String(parts[1])),
              let keyIDData = base64URLDecode(String(parts[2])),
              let keyData = base64URLDecode(String(parts[3])),
              let repoID = String(data: repoIDData, encoding: .utf8), !repoID.isEmpty,
              let keyID = String(data: keyIDData, encoding: .utf8), !keyID.isEmpty else {
            throw RepoEncryptionKeyError.invalidRecoveryKey
        }
        do {
            return try RepoEncryptionKeyMaterial(repoID: repoID, keyID: keyID, keyData: keyData)
        } catch {
            throw RepoEncryptionKeyError.invalidRecoveryKey
        }
    }

    static func keyCheck(repoID: String, keyID: String, keyData: Data) throws -> String {
        guard keyData.count == RepoEncryptionKeyMaterial.byteCount else {
            throw RepoEncryptionKeyError.invalidKeyLength
        }
        let message = Data("\(keyCheckDomain)\n\(repoID)\n\(keyID)".utf8)
        let mac = HMAC<SHA256>.authenticationCode(for: message, using: SymmetricKey(data: keyData))
        return base64URL(Data(mac))
    }

    private static func randomBytes(count: Int) throws -> Data {
        var data = Data(count: count)
        let status = data.withUnsafeMutableBytes { buffer in
            guard let baseAddress = buffer.baseAddress else { return errSecParam }
            return SecRandomCopyBytes(kSecRandomDefault, count, baseAddress)
        }
        guard status == errSecSuccess else { throw RepoEncryptionKeyError.randomGenerationFailed }
        return data
    }

    private static func base64URL(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private static func base64URLDecode(_ text: String) -> Data? {
        guard !text.isEmpty else { return nil }
        var base64 = text
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        let padding = (4 - base64.count % 4) % 4
        if padding > 0 {
            base64 += String(repeating: "=", count: padding)
        }
        return Data(base64Encoded: base64)
    }
}

struct RepoEncryptionContext: Equatable, Sendable {
    let repoID: String
    let activeKeyID: String
    let contentKey: Data

    static func verified(
        manifest: WatermelonRemoteVersionManifest,
        keyData: Data
    ) throws -> RepoEncryptionContext {
        guard VersionManifestLite.compatibility(for: manifest) == .readableWritable,
              manifest.formatVersion == VersionManifestLite.encryptedFormatVersion,
              let repoID = manifest.repoID,
              let encryption = manifest.encryption,
              let activeKeyID = encryption.activeKeyID,
              let activeKey = encryption.keys?.first(where: { $0.kid == activeKeyID }),
              let expectedKeyCheck = activeKey.keyCheck else {
            throw RepoEncryptionKeyError.missingEncryptionConfig
        }
        let actualKeyCheck = try RepoEncryptionKeyCodec.keyCheck(
            repoID: repoID,
            keyID: activeKeyID,
            keyData: keyData
        )
        guard actualKeyCheck == expectedKeyCheck else {
            throw RepoEncryptionKeyError.keyCheckMismatch
        }
        return RepoEncryptionContext(repoID: repoID, activeKeyID: activeKeyID, contentKey: keyData)
    }
}

protocol RepoEncryptionKeyStore: Sendable {
    func save(_ material: RepoEncryptionKeyMaterial) throws
    func read(repoID: String, keyID: String) throws -> RepoEncryptionKeyMaterial
    func delete(repoID: String, keyID: String) throws
}

struct RepoEncryptionKeychainStore: RepoEncryptionKeyStore {
    private static let profileAccountPrefix = "enc-profile|"

    private struct ProfileKeyReference: Codable {
        let repoID: String
        let keyID: String
    }

    let keychain: KeychainService

    init(keychain: KeychainService) {
        self.keychain = keychain
    }

    static func account(repoID: String, keyID: String) -> String {
        "enc|\(repoID)|\(keyID)"
    }

    static func profileAccount(profileID: Int64) -> String {
        "\(profileAccountPrefix)\(profileID)"
    }

    func save(_ material: RepoEncryptionKeyMaterial) throws {
        try keychain.save(data: material.keyData, account: Self.account(repoID: material.repoID, keyID: material.keyID))
    }

    func read(repoID: String, keyID: String) throws -> RepoEncryptionKeyMaterial {
        try RepoEncryptionKeyMaterial(
            repoID: repoID,
            keyID: keyID,
            keyData: keychain.readData(account: Self.account(repoID: repoID, keyID: keyID))
        )
    }

    func delete(repoID: String, keyID: String) throws {
        try keychain.delete(account: Self.account(repoID: repoID, keyID: keyID))
    }

    func saveProfileKeyReference(profileID: Int64, material: RepoEncryptionKeyMaterial) throws {
        let reference = ProfileKeyReference(repoID: material.repoID, keyID: material.keyID)
        try keychain.save(data: JSONEncoder().encode(reference), account: Self.profileAccount(profileID: profileID))
    }

    func readProfileKey(profileID: Int64) throws -> RepoEncryptionKeyMaterial {
        let data = try keychain.readData(account: Self.profileAccount(profileID: profileID))
        guard let reference = try? JSONDecoder().decode(ProfileKeyReference.self, from: data) else {
            throw RepoEncryptionKeyError.missingActiveKey
        }
        return try read(repoID: reference.repoID, keyID: reference.keyID)
    }

    func deleteProfileKeyReference(profileID: Int64) throws {
        do {
            try keychain.delete(account: Self.profileAccount(profileID: profileID))
        } catch let error as KeychainError {
            if case .unhandled(let status) = error, status == errSecItemNotFound { return }
            throw error
        }
    }

    func deleteProfileKey(profileID: Int64) throws {
        let account = Self.profileAccount(profileID: profileID)
        let data: Data
        do {
            data = try keychain.readData(account: account)
        } catch let error as KeychainError {
            if case .unhandled(let status) = error, status == errSecItemNotFound { return }
            throw error
        }

        guard let reference = try? JSONDecoder().decode(ProfileKeyReference.self, from: data) else {
            try keychain.delete(account: account)
            return
        }
        try keychain.delete(account: account)
        if try hasProfileKeyReference(to: reference, excluding: account) { return }
        try delete(repoID: reference.repoID, keyID: reference.keyID)
    }

    private func hasProfileKeyReference(to reference: ProfileKeyReference, excluding account: String) throws -> Bool {
        let profileReferences = try keychain.readDataByAccountPrefix(Self.profileAccountPrefix)
        return profileReferences.contains { otherAccount, data in
            guard otherAccount != account,
                  let otherReference = try? JSONDecoder().decode(ProfileKeyReference.self, from: data) else {
                return false
            }
            return otherReference.repoID == reference.repoID && otherReference.keyID == reference.keyID
        }
    }
}
