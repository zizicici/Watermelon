import Foundation

// Hands out a monotonic sequence stamped at progress-emission time (before the main-actor hop), so the
// sink can apply frames in emission order regardless of Task delivery order.
private final class ProgressSequencer: @unchecked Sendable {
    private let lock = NSLock()
    private var value: UInt64 = 0
    func next() -> UInt64 {
        lock.lock()
        defer { lock.unlock() }
        value &+= 1
        return value
    }
}

@MainActor
final class HomeConnectionController {

    private let dependencies: DependencyContainer
    private var didAttemptAutoConnect = false
    private var connectingProfile: ServerProfileRecord?
    private var connectTask: Task<Void, Never>?
    private var remoteRefreshTask: Task<Void, Never>?

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

    // Progress frames hop onto the main actor via independent Tasks (delivery order not guaranteed) and
    // parallel sync can emit them near-simultaneously. Stamp each connect with an epoch + per-frame
    // sequence so the UI applies them strictly in emission order and ignores stale/old-attempt frames.
    private var progressEpoch: UInt64 = 0
    private var lastProgressEpoch: UInt64 = 0
    private var lastProgressSequence: UInt64 = 0

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
            guard let password = try? dependencies.keychainService.readPassword(account: profile.credentialRef) else {
                return
            }
            try? dependencies.keychainService.save(password: password, account: profile.credentialRef)
            connect(profile: profile, password: password, reportFailure: false)
        } else {
            connect(profile: profile, password: "", reportFailure: false)
        }
    }

    func promptAndConnect(profile: ServerProfileRecord) {
        // Re-tapping the profile already connecting is a no-op; tapping a different one supersedes it.
        guard connectingProfile?.id != profile.id else { return }

        if !profile.storageProfile.requiresPassword {
            connect(profile: profile, password: "")
            return
        }

        if let saved = try? dependencies.keychainService.readPassword(account: profile.credentialRef) {
            try? dependencies.keychainService.save(password: saved, account: profile.credentialRef)
            connect(profile: profile, password: saved)
            return
        }

        guard profile.storageProfile.supportsPasswordPrompt else {
            onConnectFailed?(profile, RemoteStorageClientError.invalidConfiguration)
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
        remoteRefreshTask?.cancel()
        remoteRefreshTask = nil
        connectingProfile = nil
        clearSyncProgress()
        try? dependencies.databaseManager.setActiveServerProfileID(nil)
        dependencies.appSession.clear()
    }

    /// Refresh the connected remote's index in place (e.g. after a background backup uploaded to it).
    /// Owned here because the connection owns the shared remote cache: this task is cancelled by any
    /// connect/disconnect, and it only ever reloads the *currently connected* profile — which doesn't
    /// reset the shared cache — so it can't pollute a different connection's view. `onApplied` fires on
    /// the main actor only if we're still on the same profile when the reload lands.
    /// Known limitation: the reload applies the cache month-by-month, so an execution started mid-refresh
    /// can transiently read a partial remote view (same-profile, self-healing, dedup-protected). Accepted
    /// over a transactional cache publish, which would spike peak memory on the connect path.
    func refreshActiveRemoteIndex(onApplied: @escaping () -> Void) {
        guard connectingProfile == nil,
              let profile = dependencies.appSession.activeProfile,
              let password = profile.resolvedSessionPassword(from: dependencies.appSession) else { return }
        remoteRefreshTask?.cancel()
        remoteRefreshTask = Task { [weak self] in
            guard let self else { return }
            do {
                _ = try await self.dependencies.backupCoordinator.reloadRemoteIndex(profile: profile, password: password)
            } catch {
                return  // failed or cancelled — don't sync a reload that didn't complete
            }
            guard !Task.isCancelled,
                  self.dependencies.appSession.activeProfile?.id == profile.id else { return }
            self.remoteRefreshTask = nil
            onApplied()
        }
    }

    // Applies a connect progress frame only if it belongs to the current attempt and is newer (by emission
    // sequence) than the last applied one — so a late earlier-phase frame (e.g. a delayed `.scanningRemoteIndex`
    // landing after `.remoteIndex`) or a previous same-profile attempt's frame can't roll the UI backwards.
    private func applyOrderedSyncProgress(
        _ progress: RemoteSyncProgress,
        profile: ServerProfileRecord,
        epoch: UInt64,
        sequence: UInt64
    ) {
        guard connectingProfile?.id == profile.id, epoch == progressEpoch else { return }
        if epoch == lastProgressEpoch, sequence <= lastProgressSequence { return }
        lastProgressEpoch = epoch
        lastProgressSequence = sequence
        updateSyncProgress(progress)
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
        guard connectingProfile?.id != profile.id else { return }
        // A connect supersedes any in-flight connect (switching off a slow node) and background remote refresh.
        connectTask?.cancel()
        remoteRefreshTask?.cancel()
        remoteRefreshTask = nil
        connectingProfile = profile
        progressEpoch &+= 1
        let epoch = progressEpoch
        let sequencer = ProgressSequencer()
        clearSyncProgress()
        onStateChanged?()

        connectTask = Task { [weak self] in
            guard let self else { return }
            do {
                _ = try await self.dependencies.backupCoordinator.reloadRemoteIndex(
                    profile: profile,
                    password: password,
                    onSyncProgress: { [weak self] progress in
                        let sequence = sequencer.next()
                        Task { @MainActor [weak self] in
                            self?.applyOrderedSyncProgress(progress, profile: profile, epoch: epoch, sequence: sequence)
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

                // The failed sync may have reset the shared snapshot cache. If a previous profile is
                // still active, restore its remote index so Home keeps showing its real library.
                if let prev = self.dependencies.appSession.activeProfile {
                    var restored = false
                    if let prevPassword = prev.resolvedSessionPassword(from: self.dependencies.appSession) {
                        restored = (try? await self.dependencies.backupCoordinator.reloadRemoteIndex(
                            profile: prev,
                            password: prevPassword,
                            onSyncProgress: nil
                        )) != nil
                    }
                    guard !Task.isCancelled else { return }
                    if !restored {
                        // Cache is empty after the failed restore; disconnect rather than present the
                        // previous profile as connected-and-ready over an empty remote view.
                        self.disconnect()
                        if reportFailure {
                            self.onConnectFailed?(profile, error)
                        }
                        return
                    }
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
