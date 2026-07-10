import Foundation
import os

enum StorageProfilePersistence {
    private static let log = Logger(subsystem: "com.zizicici.watermelon", category: "ProfilePersistence")

    static func saveRemoteProfile(
        dependencies: DependencyContainer,
        profile: inout ServerProfileRecord,
        credential: String,
        replacing oldProfile: ServerProfileRecord?
    ) throws {
        let previousCredential = try? dependencies.keychainService.readPassword(account: profile.credentialRef)
        try dependencies.keychainService.save(password: credential, account: profile.credentialRef)
        do {
            try dependencies.databaseManager.saveServerProfile(&profile)
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

        if let oldRef = oldProfile?.credentialRef, oldRef != profile.credentialRef {
            deleteCredentialIfUnused(dependencies: dependencies, credentialRef: oldRef)
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
