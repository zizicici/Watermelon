import Foundation
import os
import Security

protocol StorageProfileCredentialStore: AnyObject {
    func save(password: String, account: String) throws
    func readPassword(account: String) throws -> String
    func delete(account: String) throws
}

extension KeychainService: StorageProfileCredentialStore {}

struct StorageProfileMutationService {
    private static let log = Logger(subsystem: "com.zizicici.watermelon", category: "ProfileMutation")

    let databaseManager: DatabaseManager
    let credentialStore: StorageProfileCredentialStore
    let runtimeFlags: AppRuntimeFlags

    func saveRemoteProfile(
        editingProfile: ServerProfileRecord?,
        credential: String,
        makeProfile: (ServerProfileRecord?) throws -> ServerProfileRecord
    ) throws -> ServerProfileRecord? {
        try runtimeFlags.withProfileMutationLease(profileID: editingProfile?.id) {
            let liveProfile = try StorageProfilePersistence.liveEditingProfile(
                databaseManager: databaseManager,
                snapshot: editingProfile
            )
            var profile = try makeProfile(liveProfile)
            if liveProfile == nil, profile.id != nil {
                throw RemoteStorageClientError.invalidConfiguration
            }
            try databaseManager.validateConnectionProfileSave(
                profile,
                editingProfileID: liveProfile?.id
            )
            try persist(
                profile: &profile,
                credential: credential,
                replacing: liveProfile
            )
            return profile
        }
    }

    private func persist(
        profile: inout ServerProfileRecord,
        credential: String,
        replacing oldProfile: ServerProfileRecord?
    ) throws {
        let replacedCredentialRef = oldProfile?.credentialRef
        let previousCredential: String?
        do {
            previousCredential = try credentialStore.readPassword(account: profile.credentialRef)
        } catch KeychainError.unhandled(let status) where status == errSecItemNotFound {
            previousCredential = nil
        }

        try credentialStore.save(password: credential, account: profile.credentialRef)
        do {
            try databaseManager.saveConnectionProfile(
                &profile,
                editingProfileID: oldProfile?.id
            )
        } catch {
            do {
                if let previousCredential {
                    try credentialStore.save(password: previousCredential, account: profile.credentialRef)
                } else {
                    try credentialStore.delete(account: profile.credentialRef)
                }
            } catch {
                Self.log.error("Failed to roll back credential after profile save failure: \(String(describing: error), privacy: .public)")
            }
            throw error
        }

        if let replacedCredentialRef, replacedCredentialRef != profile.credentialRef {
            deleteCredentialIfUnused(replacedCredentialRef)
        }
    }

    private func deleteCredentialIfUnused(_ credentialRef: String) {
        do {
            let stillUsed = try databaseManager.fetchServerProfiles().contains {
                $0.credentialRef == credentialRef
            }
            guard !stillUsed else { return }
            try credentialStore.delete(account: credentialRef)
        } catch {
            Self.log.error("Failed to clean up unused credential: \(String(describing: error), privacy: .public)")
        }
    }
}

extension DependencyContainer {
    var storageProfileMutationService: StorageProfileMutationService {
        StorageProfileMutationService(
            databaseManager: databaseManager,
            credentialStore: keychainService,
            runtimeFlags: appRuntimeFlags
        )
    }
}
