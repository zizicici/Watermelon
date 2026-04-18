import Foundation

@MainActor
final class HomeConnectionController {

    private let dependencies: DependencyContainer
    private var didAttemptAutoConnect = false
    private var connectingProfile: ServerProfileRecord?
    private var connectTask: Task<Void, Never>?

    var state: ConnectionState {
        if let profile = connectingProfile {
            return .connecting(profile)
        }
        if let profile = dependencies.appSession.activeProfile {
            return .connected(profile)
        }
        return .disconnected
    }

    private(set) var savedProfiles: [ServerProfileRecord] = []
    private(set) var syncProgress: RemoteSyncProgress?

    var onStateChanged: (() -> Void)?
    var onSyncProgressChanged: (() -> Void)?
    var onNeedsPasswordPrompt: ((ServerProfileRecord, _ completion: @escaping (String) -> Void) -> Void)?
    var onConnectFailed: ((ServerProfileRecord, Error) -> Void)?

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        dependencies.appSession.onSessionChanged = { [weak self] _ in
            guard let self else { return }
            self.loadProfiles()
            self.onStateChanged?()
        }
    }

    func loadProfiles() {
        savedProfiles = (try? dependencies.databaseManager.fetchServerProfiles()) ?? []
    }

    func attemptAutoConnect() {
        guard !didAttemptAutoConnect else { return }
        didAttemptAutoConnect = true

        let activeID = try? dependencies.databaseManager.activeServerProfileID()
        guard let activeID,
              let profile = savedProfiles.first(where: { $0.id == activeID }) else {
            return
        }

        if profile.storageProfile.requiresPassword {
            guard let password = try? dependencies.keychainService.readPassword(account: profile.credentialRef),
                  !password.isEmpty else {
                return
            }
            connect(profile: profile, password: password, reportFailure: false)
        } else {
            connect(profile: profile, password: "", reportFailure: false)
        }
    }

    func promptAndConnect(profile: ServerProfileRecord) {
        guard connectingProfile == nil else { return }

        if !profile.storageProfile.requiresPassword {
            connect(profile: profile, password: "")
            return
        }

        if let saved = try? dependencies.keychainService.readPassword(account: profile.credentialRef),
           !saved.isEmpty {
            connect(profile: profile, password: saved)
            return
        }

        onNeedsPasswordPrompt?(profile) { [weak self] password in
            try? self?.dependencies.keychainService.save(password: password, account: profile.credentialRef)
            self?.connect(profile: profile, password: password)
        }
    }

    func disconnect() {
        connectTask?.cancel()
        connectTask = nil
        connectingProfile = nil
        clearSyncProgress()
        try? dependencies.databaseManager.setActiveServerProfileID(nil)
        dependencies.appSession.clear()
    }

    private func updateSyncProgress(_ progress: RemoteSyncProgress?) {
        guard syncProgress != progress else { return }
        syncProgress = progress
        onSyncProgressChanged?()
    }

    private func clearSyncProgress() {
        updateSyncProgress(nil)
    }

    func resolvedSessionPassword(for profile: ServerProfileRecord) -> String? {
        profile.resolvedSessionPassword(from: dependencies.appSession)
    }

    // MARK: - Internal

    private func connect(profile: ServerProfileRecord, password: String, reportFailure: Bool = true) {
        guard connectingProfile == nil else { return }
        connectingProfile = profile
        clearSyncProgress()
        onStateChanged?()

        connectTask = Task { [weak self] in
            guard let self else { return }
            do {
                _ = try await self.dependencies.backupCoordinator.reloadRemoteIndex(
                    profile: profile,
                    password: password,
                    onSyncProgress: { [weak self] progress in
                        Task { @MainActor [weak self] in
                            guard let self, self.connectingProfile?.id == profile.id else { return }
                            self.updateSyncProgress(progress)
                        }
                    }
                )
                guard !Task.isCancelled else { return }
                try self.dependencies.databaseManager.setActiveServerProfileID(profile.id)
                self.connectingProfile = nil
                self.connectTask = nil
                self.dependencies.appSession.activate(profile: profile, password: password)
                self.clearSyncProgress()
            } catch {
                guard !Task.isCancelled else { return }

                // The failed sync may have reset the shared snapshot cache.
                // If a previous profile is still active, restore its remote index.
                if let prev = self.dependencies.appSession.activeProfile,
                   let prevPassword = prev.resolvedSessionPassword(from: self.dependencies.appSession) {
                    _ = try? await self.dependencies.backupCoordinator.reloadRemoteIndex(
                        profile: prev,
                        password: prevPassword,
                        onSyncProgress: nil
                    )
                }

                self.connectingProfile = nil
                self.connectTask = nil
                self.clearSyncProgress()
                self.onStateChanged?()
                if reportFailure {
                    self.onConnectFailed?(profile, error)
                }
            }
        }
    }
}
