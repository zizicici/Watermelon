import Foundation

@MainActor
final class HomeConnectionController {

    private let dependencies: DependencyContainer
    private var didAttemptAutoConnect = false

    private(set) var state: ConnectionState = .disconnected
    private(set) var savedProfiles: [ServerProfileRecord] = []

    var onStateChanged: (() -> Void)?
    var onMonthSynced: (() -> Void)?
    var onNeedsPasswordPrompt: ((ServerProfileRecord, _ completion: @escaping (String) -> Void) -> Void)?
    var onConnectFailed: ((ServerProfileRecord, Error) -> Void)?

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        dependencies.appSession.onSessionChanged = { [weak self] _ in
            guard let self else { return }
            if let profile = self.dependencies.appSession.activeProfile {
                self.state = .connected(profile)
            } else if !self.state.isConnecting {
                self.state = .disconnected
            }
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
        guard !state.isConnecting else { return }

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
        state = .disconnected
        try? dependencies.databaseManager.setActiveServerProfileID(nil)
        dependencies.appSession.clear()
    }

    func resolvedSessionPassword(for profile: ServerProfileRecord) -> String? {
        profile.resolvedSessionPassword(from: dependencies.appSession)
    }

    // MARK: - Internal

    private func connect(profile: ServerProfileRecord, password: String, reportFailure: Bool = true) {
        guard !state.isConnecting else { return }
        state = .connecting
        onStateChanged?()

        Task { [weak self] in
            guard let self else { return }
            do {
                _ = try await self.dependencies.backupCoordinator.reloadRemoteIndex(
                    profile: profile,
                    password: password,
                    onMonthSynced: { [weak self] in
                        Task { @MainActor [weak self] in
                            self?.onMonthSynced?()
                        }
                    }
                )
                try self.dependencies.databaseManager.setActiveServerProfileID(profile.id)
                self.state = .connected(profile)
                self.dependencies.appSession.activate(profile: profile, password: password)
                // appSession.onSessionChanged fires loadProfiles + onStateChanged
            } catch {
                self.state = .disconnected
                self.onStateChanged?()
                if reportFailure {
                    self.onConnectFailed?(profile, error)
                }
            }
        }
    }
}

extension ConnectionState {
    var isConnecting: Bool {
        if case .connecting = self { return true }
        return false
    }
}
