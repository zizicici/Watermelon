import Foundation
import os

enum OneDriveCachedAccountRetentionPolicy {
    static func shouldRemove(
        deletedHomeAccountIdentifier: String,
        remainingCredentials: [OneDriveCredentialBlob]
    ) -> Bool {
        !remainingCredentials.contains {
            $0.homeAccountIdentifier == deletedHomeAccountIdentifier
        }
    }
}

final class OneDrivePendingAccountRegistry: @unchecked Sendable {
    private let lock = NSLock()
    private var countsByHomeAccountIdentifier: [String: Int] = [:]

    func retain(homeAccountIdentifier: String) {
        lock.withLock {
            countsByHomeAccountIdentifier[homeAccountIdentifier, default: 0] += 1
        }
    }

    @discardableResult
    func release(homeAccountIdentifier: String) -> Bool {
        lock.withLock {
            guard let count = countsByHomeAccountIdentifier[homeAccountIdentifier], count > 0 else {
                return false
            }
            if count == 1 {
                countsByHomeAccountIdentifier.removeValue(forKey: homeAccountIdentifier)
                return true
            }
            countsByHomeAccountIdentifier[homeAccountIdentifier] = count - 1
            return false
        }
    }

    func contains(homeAccountIdentifier: String) -> Bool {
        lock.withLock { countsByHomeAccountIdentifier[homeAccountIdentifier] != nil }
    }

    func performIfUnretained(
        homeAccountIdentifier: String,
        _ action: () -> Void
    ) {
        lock.withLock {
            guard countsByHomeAccountIdentifier[homeAccountIdentifier] == nil else { return }
            action()
        }
    }
}

final class OneDriveCredentialLifecycleService: @unchecked Sendable {
    private static let log = Logger(subsystem: "com.zizicici.watermelon", category: "OneDriveCredential")
    private static let pendingAccounts = OneDrivePendingAccountRegistry()

    private let databaseManager: DatabaseManager
    private let keychainService: KeychainService
    private let authenticationService: OneDriveMSALService

    init(
        databaseManager: DatabaseManager,
        keychainService: KeychainService,
        authenticationService: OneDriveMSALService
    ) {
        self.databaseManager = databaseManager
        self.keychainService = keychainService
        self.authenticationService = authenticationService
    }

    func makePendingAccountLease(
        credential: OneDriveCredentialBlob
    ) -> PendingOneDriveAccountLease {
        Self.pendingAccounts.retain(homeAccountIdentifier: credential.homeAccountIdentifier)
        return PendingOneDriveAccountLease(
            credential: credential,
            finalize: { [self] credential, disposition in
                let wasLastPending = Self.pendingAccounts.release(
                    homeAccountIdentifier: credential.homeAccountIdentifier
                )
                if disposition == .discarded, wasLastPending {
                    removeCachedAccountIfUnused(credential: credential)
                }
            }
        )
    }

    func removeCachedAccountIfUnused(credentialJSONString: String) {
        guard let credential = try? OneDriveCredentialBlob.decode(from: credentialJSONString) else { return }
        removeCachedAccountIfUnused(credential: credential)
    }

    func removeCachedAccountIfUnused(credential: OneDriveCredentialBlob) {
        Self.pendingAccounts.performIfUnretained(
            homeAccountIdentifier: credential.homeAccountIdentifier
        ) { [self] in
            removeCachedAccountIfUnusedAfterPendingCheck(credential: credential)
        }
    }

    func reconcileCachedAccounts() {
        guard let profiles = try? databaseManager.fetchServerProfiles() else { return }
        var retainedIdentifiers = Set<String>()
        for profile in profiles where profile.resolvedStorageType == .onedrive {
            guard let raw = try? keychainService.readPassword(account: profile.credentialRef),
                  let credential = try? OneDriveCredentialBlob.decode(from: raw) else { return }
            retainedIdentifiers.insert(credential.homeAccountIdentifier)
        }
        guard let cachedIdentifiers = try? authenticationService.cachedHomeAccountIdentifiers() else { return }
        for identifier in cachedIdentifiers where !retainedIdentifiers.contains(identifier) {
            Self.pendingAccounts.performIfUnretained(homeAccountIdentifier: identifier) { [self] in
                do {
                    try authenticationService.removeCachedAccount(homeAccountIdentifier: identifier)
                } catch {
                    Self.log.error("Failed to reconcile an unused OneDrive account")
                }
            }
        }
    }

    private func removeCachedAccountIfUnusedAfterPendingCheck(credential: OneDriveCredentialBlob) {
        guard let profiles = try? databaseManager.fetchServerProfiles() else { return }
        var remainingCredentials: [OneDriveCredentialBlob] = []
        for profile in profiles where profile.resolvedStorageType == .onedrive {
            guard let raw = try? keychainService.readPassword(account: profile.credentialRef),
                  let credential = try? OneDriveCredentialBlob.decode(from: raw) else { return }
            remainingCredentials.append(credential)
        }
        guard OneDriveCachedAccountRetentionPolicy.shouldRemove(
            deletedHomeAccountIdentifier: credential.homeAccountIdentifier,
            remainingCredentials: remainingCredentials
        ) else { return }
        do {
            try authenticationService.removeCachedAccount(
                homeAccountIdentifier: credential.homeAccountIdentifier
            )
        } catch {
            Self.log.error("Failed to remove an unused OneDrive account from the local token cache")
        }
    }
}
