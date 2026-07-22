import CryptoKit
import Foundation
import os
import UIKit

struct StorageProfileCommitGate {
    private(set) var isCommitting = false

    mutating func begin() -> Bool {
        guard !isCommitting else { return false }
        isCommitting = true
        return true
    }

    mutating func releaseAfterFailure() {
        end()
    }

    mutating func end() {
        isCommitting = false
    }
}

enum ExternalStorageSaveCompletionMode: Equatable {
    case none
    case deferred
    case normal
    case refreshOnly
}

enum ExternalStorageScreenPhase: Equatable {
    case active
    case departing
    case inactive
}

enum ExternalStorageSaveCompletionPolicy {
    static func mode(
        commitSucceeded: Bool,
        operationIsCurrent: Bool,
        screenPhase: ExternalStorageScreenPhase,
        isScreenActive: Bool
    ) -> ExternalStorageSaveCompletionMode {
        guard commitSucceeded else { return .none }
        guard operationIsCurrent else { return .refreshOnly }
        switch screenPhase {
        case .active:
            return isScreenActive ? .normal : .refreshOnly
        case .departing:
            return .deferred
        case .inactive:
            return .refreshOnly
        }
    }

    static func shouldEndCommitGate(
        mode: ExternalStorageSaveCompletionMode,
        operationIsCurrent: Bool
    ) -> Bool {
        operationIsCurrent && mode == .refreshOnly
    }

}

@MainActor
enum ExternalStoragePersistedProfileRefresh {
    static func applyToActiveSession(
        appSession: AppSession,
        originalProfile: ServerProfileRecord,
        savedProfile: ServerProfileRecord
    ) {
        guard appSession.activeProfile?.id == originalProfile.id else { return }
        guard originalProfile.hasSameRemoteDestination(as: savedProfile),
              let password = appSession.activePassword else {
            appSession.clear()
            return
        }
        appSession.activate(profile: savedProfile, password: password)
    }
}

struct SMBSelectionContextSignature: Equatable, Sendable {
    let host: String
    let port: Int
    let username: String
    let passwordDigest: Data
    let domain: String

    init(auth: SMBServerAuthContext) {
        host = RemoteHostIdentity.canonicalSMB(auth.host)
        port = SMBEndpoint.effectivePort(auth.port)
        username = auth.username.trimmingCharacters(in: .whitespacesAndNewlines)
        passwordDigest = Data(SHA256.hash(data: Data(auth.password.utf8)))
        domain = (auth.domain ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
    }
}

struct SMBSelectionContextBinding {
    private(set) var signature: SMBSelectionContextSignature?

    var isBound: Bool { signature != nil }

    mutating func bind(to signature: SMBSelectionContextSignature) {
        self.signature = signature
    }

    func matches(_ signature: SMBSelectionContextSignature) -> Bool {
        self.signature == signature
    }

    mutating func invalidateIfMismatched(_ current: SMBSelectionContextSignature?) -> Bool {
        guard signature != nil, signature != current else { return false }
        signature = nil
        return true
    }

    mutating func clear() {
        signature = nil
    }
}

@MainActor
enum StorageProfileSaveTransition {
    static func completeCreate(
        from viewController: UIViewController,
        shouldPopToRoot: Bool,
        completion: @escaping () -> Void
    ) {
        guard shouldPopToRoot else {
            completion()
            return
        }
        guard let navigationController = viewController.navigationController else {
            completion()
            return
        }
        let popped = navigationController.popToRootViewController(animated: true)
        guard popped?.isEmpty == false,
              let coordinator = navigationController.transitionCoordinator else {
            completion()
            return
        }
        let registered = coordinator.animate(alongsideTransition: nil) { _ in completion() }
        if !registered {
            completion()
        }
    }
}

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

    static func credentialRef(for identity: ProfileDuplicateIdentity) -> String {
        credentialRef(storageType: identity.storageType, identityFields: identity.components)
    }

    static func liveEditingProfile(
        databaseManager: DatabaseManager,
        snapshot: ServerProfileRecord?
    ) throws -> ServerProfileRecord? {
        guard let snapshot else { return nil }
        guard let profileID = snapshot.id,
              let liveProfile = try databaseManager.fetchServerProfile(id: profileID) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return liveProfile
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

    static func cleanupCredentialsAfterProfileDeletion(
        dependencies: DependencyContainer,
        profile: ServerProfileRecord
    ) {
        let oneDriveCredential = profile.resolvedStorageType == .onedrive
            ? (try? dependencies.keychainService.readPassword(account: profile.credentialRef))
            : nil
        if profile.storageProfile.requiresStoredCredential {
            deleteCredentialIfUnused(dependencies: dependencies, credentialRef: profile.credentialRef)
        }
        if let oneDriveCredential {
            dependencies.oneDriveCredentialLifecycleService.removeCachedAccountIfUnused(
                credentialJSONString: oneDriveCredential
            )
        }
    }
}
