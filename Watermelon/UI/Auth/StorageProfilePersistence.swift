import CryptoKit
import Foundation
import os
import Security

enum StorageProfilePersistence {
    private static let log = Logger(subsystem: "com.zizicici.watermelon", category: "ProfilePersistence")

    static func credentialRef(storageType: StorageType, identityFields: [String]) -> String {
        var payload = Data()
        for field in identityFields {
            let fieldData = Data(field.utf8)
            var length = UInt64(fieldData.count).bigEndian
            withUnsafeBytes(of: &length) { payload.append(contentsOf: $0) }
            payload.append(fieldData)
        }
        let digest = SHA256.hash(data: payload).map { String(format: "%02x", $0) }.joined()
        return "v2|\(storageType.rawValue)|\(digest)"
    }

    static func saveRemoteProfile(
        dependencies: DependencyContainer,
        profile: inout ServerProfileRecord,
        credential: String,
        replacing oldProfile: ServerProfileRecord?
    ) throws {
        let replacedCredentialRef: String?
        if let id = oldProfile?.id {
            replacedCredentialRef = try dependencies.databaseManager.fetchServerProfile(id: id)?.credentialRef
                ?? oldProfile?.credentialRef
        } else {
            replacedCredentialRef = oldProfile?.credentialRef
        }

        let previousCredential: String?
        do {
            previousCredential = try dependencies.keychainService.readPassword(account: profile.credentialRef)
        } catch KeychainError.unhandled(let status) where status == errSecItemNotFound {
            previousCredential = nil
        }
        try dependencies.keychainService.save(password: credential, account: profile.credentialRef)
        do {
            try dependencies.databaseManager.saveConnectionProfile(
                &profile,
                editingProfileID: oldProfile?.id
            )
        } catch {
            do {
                if let previousCredential {
                    try dependencies.keychainService.save(password: previousCredential, account: profile.credentialRef)
                } else {
                    try dependencies.keychainService.delete(account: profile.credentialRef)
                }
            } catch {
                log.error("Failed to roll back credential after profile save failure: \(String(describing: error), privacy: .public)")
            }
            throw error
        }

        if let replacedCredentialRef, replacedCredentialRef != profile.credentialRef {
            deleteCredentialIfUnused(dependencies: dependencies, credentialRef: replacedCredentialRef)
        }
    }

    static func deleteCredentialIfUnused(dependencies: DependencyContainer, credentialRef: String) {
        do {
            let stillUsed = try dependencies.databaseManager.fetchServerProfiles().contains {
                $0.credentialRef == credentialRef
            }
            guard !stillUsed else { return }
            try dependencies.keychainService.delete(account: credentialRef)
        } catch {
            log.error("Failed to clean up unused credential: \(String(describing: error), privacy: .public)")
        }
    }
}
