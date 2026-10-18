import Foundation
import os
import UIKit

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
    private let applicationState: @MainActor () -> UIApplication.State
    private let reloadRemoteIndex: @MainActor (ServerProfileRecord, String) async throws -> Void
    private var remoteReloadWatermark: Date?
    private var connectingProfile: ServerProfileRecord?
    private var connectTask: Task<Void, Never>?
    private var remoteRefreshTask: Task<Void, Never>?
    private var remoteSnapshotObserverID: UUID?
    private let reconciliationObservers = NotificationObserverBag()

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
    private var isAutoConnectSuppressedForBrowserLink = false
    private var isAutoConnectSuperseded = false

    var onStateChanged: (() -> Void)?
    var onProfilesChanged: (() -> Void)?
    var onSyncProgressChanged: (() -> Void)?
    var onNeedsPasswordPrompt: ((ServerProfileRecord, _ completion: @escaping (String) -> Void) -> Void)?
    var onNeedsSFTPHostKeyTrust: ((ServerProfileRecord, SFTPHostKeyPromptPolicy.Decision, String) async -> Bool)?
    var onConnectFailed: ((ServerProfileRecord, Error) -> Void)?

    init(
        dependencies: DependencyContainer,
        applicationState: @escaping @MainActor () -> UIApplication.State = { UIApplication.shared.applicationState },
        reloadRemoteIndex: (@MainActor (ServerProfileRecord, String) async throws -> Void)? = nil
    ) {
        self.dependencies = dependencies
        self.applicationState = applicationState
        self.reloadRemoteIndex = reloadRemoteIndex ?? { profile, credential in
            _ = try await dependencies.backupCoordinator.reloadRemoteIndex(profile: profile, password: credential)
        }
        dependencies.appSession.onSessionChanged = { [weak self] _ in
            guard let self else { return }
            self.loadProfiles()
            self.onStateChanged?()
            self.refreshRemoteIndexIfNeeded()
        }
        remoteSnapshotObserverID = dependencies.backupCoordinator.remoteSnapshotChanges.addCoalescedMainActorObserver { [weak self] in
            self?.refreshRemoteIndexIfNeeded()
        }
        for name in [Notification.Name.ExecutionLifecycleDidChange, .RemoteMaintenanceDidChange, .BackgroundBackupRunMarkerDidChange,
                     UIApplication.didBecomeActiveNotification] {
            reconciliationObservers.insert(NotificationCenter.default.addObserver(forName: name, object: nil, queue: .main) { [weak self] _ in
                MainActor.assumeIsolated { self?.refreshRemoteIndexIfNeeded() }
            })
        }
        reconciliationObservers.insert(NotificationCenter.default.addObserver(
            forName: UIApplication.willResignActiveNotification, object: nil, queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated { self?.cancelRemoteRefresh() }
        })
    }

    deinit {
        if let remoteSnapshotObserverID {
            dependencies.backupCoordinator.remoteSnapshotChanges.removeObserver(remoteSnapshotObserverID)
        }
    }

    private func cancelRemoteRefresh() {
        remoteRefreshTask?.cancel()
        remoteRefreshTask = nil
    }

    func refreshRemoteIndexIfNeeded() {
        guard applicationState() == .active, !dependencies.appRuntimeFlags.isExecuting,
              !dependencies.remoteMaintenanceController.isBusy else {
            cancelRemoteRefresh()
            return
        }
        guard remoteRefreshTask == nil, connectingProfile == nil,
              let profile = dependencies.appSession.activeProfile,
              let credentialPayload = profile.resolvedSessionCredential(from: dependencies.appSession) else { return }
        let marker = Self.latestBackgroundRun(dependencies.databaseManager, profileID: profile.id)
        guard dependencies.backupCoordinator.remoteSnapshotRequiresReconciliation ||
              (marker ?? .distantPast) > (remoteReloadWatermark ?? .distantPast) else { return }
        remoteRefreshTask = Task { [weak self] in
            guard let self else { return }
            var didReload = false
            defer {
                if !Task.isCancelled {
                    self.remoteRefreshTask = nil
                    if didReload { self.refreshRemoteIndexIfNeeded() }
                }
            }
            do {
                try await self.reloadRemoteIndex(profile, credentialPayload)
            } catch {
                return  // failed or cancelled — don't sync a reload that didn't complete
            }
            didReload = true
            guard !Task.isCancelled,
                  self.dependencies.appSession.activeProfile?.runtimeConnectionIdentity == profile.runtimeConnectionIdentity else { return }
            self.remoteReloadWatermark = marker
        }
    }

    static func latestBackgroundRun(_ db: DatabaseManager, profileID: Int64?) -> Date? {
        guard let profileID else { return nil }
        let ran = try? db.backgroundBackupLastRanAt(profileID: profileID)
        let completed = try? db.backgroundBackupLastCompletedAt(profileID: profileID)
        return [ran, completed].compactMap { $0 }.max()
    }

    func loadProfiles() {
        savedProfiles = (try? dependencies.databaseManager.fetchServerProfiles()) ?? []
    }

    func connectBrowserLink(
        profile: ServerProfileRecord,
        completion: @escaping (Result<Void, Error>) -> Void
    ) {
        isAutoConnectSuperseded = true
        let sessionID = profile.credentialRef
        guard dependencies.appRuntimeFlags.tryBeginEphemeralConnecting(sessionID: sessionID) else {
            completion(.failure(RemoteStorageClientError.unavailable))
            return
        }
        guard connectingProfile == nil, dependencies.appSession.activeProfile == nil else {
            dependencies.appRuntimeFlags.endEphemeralConnecting(sessionID: sessionID)
            browserLinkLog.error("Home rejected temporary profile because another connection is active")
            completion(.failure(RemoteStorageClientError.unavailable))
            return
        }
        browserLinkLog.info("Home temporary profile index reload started")
        connectTask?.cancel()
        progressEpoch &+= 1
        let epoch = progressEpoch
        let sequencer = ProgressSequencer()
        let marker = Self.latestBackgroundRun(dependencies.databaseManager, profileID: profile.id)
        connectingProfile = profile
        clearSyncProgress()
        onStateChanged?()
        connectTask = Task { [weak self] in
            guard let self else { return }
            do {
                _ = try await self.dependencies.backupCoordinator.reloadRemoteIndex(
                    profile: profile,
                    password: "",
                    onSyncProgress: { [weak self] progress in
                        let sequence = sequencer.next()
                        Task { @MainActor [weak self] in
                            self?.applyOrderedSyncProgress(progress, profile: profile, epoch: epoch, sequence: sequence)
                        }
                    }
                )
                guard !Task.isCancelled,
                      self.connectingProfile?.credentialRef == profile.credentialRef,
                      self.progressEpoch == epoch else {
                    throw CancellationError()
                }
                self.connectingProfile = nil
                self.connectTask = nil
                self.remoteReloadWatermark = marker
                self.dependencies.appSession.activate(profile: profile, password: "")
                self.dependencies.appRuntimeFlags.endEphemeralConnecting(sessionID: sessionID)
                self.clearSyncProgress()
                browserLinkLog.info("Home temporary profile activated")
                completion(.success(()))
            } catch {
                guard self.connectingProfile?.credentialRef == profile.credentialRef,
                      self.progressEpoch == epoch else { return }
                self.connectingProfile = nil
                self.connectTask = nil
                self.dependencies.appRuntimeFlags.endEphemeralConnecting(sessionID: sessionID)
                self.clearSyncProgress()
                self.onStateChanged?()
                browserLinkLog.error("Home temporary profile activation failed type=\(String(reflecting: type(of: error)), privacy: .public) message=\(error.localizedDescription, privacy: .public)")
                completion(.failure(error))
            }
        }
    }

    func cancelBrowserLinkConnection(sessionID: String) {
        guard let profile = connectingProfile,
              profile.browserLinkSessionID == sessionID else { return }
        connectTask?.cancel()
        connectTask = nil
        connectingProfile = nil
        dependencies.appRuntimeFlags.endEphemeralConnecting(sessionID: profile.runtimeConnectionIdentity)
        clearSyncProgress()
        onStateChanged?()
    }

    func suppressAutoConnectForBrowserLink() {
        // The ephemeral flow owns this launch and must not reconnect a saved destination afterward.
        isAutoConnectSuppressedForBrowserLink = true
    }

    func attemptAutoConnect() -> HomeAutoConnectCoordinator.AttemptResult {
        guard !isAutoConnectSuppressedForBrowserLink, !isAutoConnectSuperseded else { return .finished }
        guard !AppRuntimeFlags.hasActiveShortcutSession,
              applicationState() == .active,
              !dependencies.appRuntimeFlags.isExecuting else { return .deferred }
        guard connectingProfile == nil, dependencies.appSession.activeProfile == nil else { return .finished }

        let activeID = try? dependencies.databaseManager.activeServerProfileID()
        guard let activeID,
              let profile = savedProfiles.first(where: { $0.id == activeID }) else {
            return .finished
        }

        if profile.storageProfile.requiresStoredCredential {
            guard let password = try? dependencies.keychainService.readPassword(account: profile.credentialRef) else {
                return .finished
            }
            try? dependencies.keychainService.save(password: password, account: profile.credentialRef)
            return connect(profile: profile, password: password, reportFailure: false, isAutomatic: true) ? .finished : .deferred
        } else {
            return connect(profile: profile, password: "", reportFailure: false, isAutomatic: true) ? .finished : .deferred
        }
    }

    func promptAndConnect(profile: ServerProfileRecord) {
        isAutoConnectSuperseded = true
        // Re-tapping the profile already connecting is a no-op; tapping a different one supersedes it.
        guard connectingProfile?.id != profile.id else { return }

        if !profile.storageProfile.requiresStoredCredential {
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
        isAutoConnectSuperseded = true
        let wasBrowserLink = connectingProfile?.isBrowserLinkProfile == true ||
            dependencies.appSession.activeProfile?.isBrowserLinkProfile == true
        let ephemeralSessionID = connectingProfile?.isBrowserLinkProfile == true
            ? connectingProfile?.credentialRef
            : nil
        let previousConnectingID = connectingProfile?.id
        connectTask?.cancel()
        connectTask = nil
        cancelRemoteRefresh()
        remoteReloadWatermark = nil
        connectingProfile = nil
        dependencies.appRuntimeFlags.endConnecting(profileID: previousConnectingID)
        if let ephemeralSessionID {
            dependencies.appRuntimeFlags.endEphemeralConnecting(sessionID: ephemeralSessionID)
        }
        clearSyncProgress()
        if !wasBrowserLink {
            try? dependencies.databaseManager.setActiveServerProfileID(nil)
        }
        dependencies.appSession.clear()
    }

    func disconnectBrowserLink() {
        guard connectingProfile?.isBrowserLinkProfile == true ||
                dependencies.appSession.activeProfile?.isBrowserLinkProfile == true else { return }
        disconnect()
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
        guard connectingProfile?.runtimeConnectionIdentity == profile.runtimeConnectionIdentity,
              epoch == progressEpoch else { return }
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

    func resolvedSessionCredential(for profile: ServerProfileRecord) -> String? {
        profile.resolvedSessionCredential(from: dependencies.appSession)
    }

    // MARK: - Internal

    @discardableResult
    private func connect(
        profile: ServerProfileRecord,
        password: String,
        reportFailure: Bool = true,
        isAutomatic: Bool = false
    ) -> Bool {
        guard connectingProfile?.id != profile.id else { return false }
        guard dependencies.appSession.activeProfile?.isBrowserLinkProfile != true else {
            if reportFailure { onConnectFailed?(profile, RemoteStorageClientError.unavailable) }
            return false
        }
        guard dependencies.appRuntimeFlags.tryBeginConnecting(profileID: profile.id, isAutomatic: isAutomatic) else {
            if reportFailure {
                onConnectFailed?(profile, RemoteStorageClientError.unavailable)
            }
            return false
        }
        // A connect supersedes any in-flight connect (switching off a slow node) and background remote refresh.
        connectTask?.cancel()
        cancelRemoteRefresh()
        remoteReloadWatermark = nil
        connectingProfile = profile
        progressEpoch &+= 1
        let epoch = progressEpoch
        let sequencer = ProgressSequencer()
        clearSyncProgress()
        onStateChanged?()

        connectTask = Task { [weak self] in
            guard let self else { return }
            let totalStart = CFAbsoluteTimeGetCurrent()
            Self.traceConnect(
                "start",
                profile: profile,
                duration: nil,
                extra: "reportFailure=\(reportFailure)"
            )
            var failureProfile = profile
            do {
                let prepareStart = CFAbsoluteTimeGetCurrent()
                let connectionProfile = try await self.dependencies.storageProfileConnectionService.prepareForConnection(
                    profile: profile,
                    confirmSFTPHostKey: { [weak self] decision, actual in
                        guard let self, let confirm = self.onNeedsSFTPHostKeyTrust else { return false }
                        return await confirm(profile, decision, actual)
                    }
                )
                Self.traceConnect(
                    "prepareForConnection",
                    profile: connectionProfile,
                    duration: Self.elapsedSeconds(since: prepareStart),
                    extra: connectionProfile.connectionParams == profile.connectionParams ? "changed=false" : "changed=true"
                )
                if connectionProfile.connectionParams != profile.connectionParams {
                    failureProfile = connectionProfile
                    let adoptStart = CFAbsoluteTimeGetCurrent()
                    self.adoptPersistedConnectionProfile(connectionProfile)
                    Self.traceConnect(
                        "adoptPersistedConnectionProfile",
                        profile: connectionProfile,
                        duration: Self.elapsedSeconds(since: adoptStart)
                    )
                }
                let marker = Self.latestBackgroundRun(self.dependencies.databaseManager, profileID: connectionProfile.id)
                let reloadStart = CFAbsoluteTimeGetCurrent()
                _ = try await self.dependencies.backupCoordinator.reloadRemoteIndex(
                    profile: connectionProfile,
                    password: password,
                    onSyncProgress: { [weak self] progress in
                        let sequence = sequencer.next()
                        Task { @MainActor [weak self] in
                            self?.applyOrderedSyncProgress(progress, profile: profile, epoch: epoch, sequence: sequence)
                        }
                    }
                )
                Self.traceConnect(
                    "reloadRemoteIndex",
                    profile: connectionProfile,
                    duration: Self.elapsedSeconds(since: reloadStart)
                )
                guard !Task.isCancelled else { return }
                guard self.connectingProfile?.id == profile.id, epoch == self.progressEpoch else { return }
                let liveFetchStart = CFAbsoluteTimeGetCurrent()
                guard let liveProfile = try self.dependencies.databaseManager.fetchServerProfiles().first(where: { $0.id == profile.id }) else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
                Self.traceConnect(
                    "fetchLiveProfile",
                    profile: liveProfile,
                    duration: Self.elapsedSeconds(since: liveFetchStart)
                )
                let acceptedBookmarkRefresh = connectionProfile.resolvedStorageType == .externalVolume &&
                    connectionProfile.id.map {
                        self.dependencies.databaseManager.matchesAcceptedExternalBookmarkRefresh(
                            profileID: $0,
                            previousConnectionParams: connectionProfile.connectionParams,
                            currentConnectionParams: liveProfile.connectionParams
                        )
                    } == true
                guard liveProfile.hasSameRemoteDestination(as: connectionProfile) || acceptedBookmarkRefresh else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
                let activateStart = CFAbsoluteTimeGetCurrent()
                try self.dependencies.databaseManager.setActiveServerProfileID(liveProfile.id)
                self.dependencies.appRuntimeFlags.endConnecting(profileID: profile.id)
                self.connectingProfile = nil
                self.connectTask = nil
                self.remoteReloadWatermark = marker
                self.dependencies.appSession.activate(profile: liveProfile, password: password)
                self.clearSyncProgress()
                Self.traceConnect(
                    "activate",
                    profile: liveProfile,
                    duration: Self.elapsedSeconds(since: activateStart)
                )
                Self.traceConnect(
                    "complete",
                    profile: liveProfile,
                    duration: Self.elapsedSeconds(since: totalStart)
                )
            } catch {
                Self.traceConnect(
                    "failed",
                    profile: failureProfile,
                    duration: Self.elapsedSeconds(since: totalStart),
                    extra: "error=\((error as NSError).localizedDescription)"
                )
                guard !Task.isCancelled else { return }
                guard self.connectingProfile?.id == profile.id, epoch == self.progressEpoch else { return }
                if error is CancellationError || RemoteFaultLite.classify(error) == .cancelled {
                    self.connectingProfile = nil
                    self.connectTask = nil
                    self.dependencies.appRuntimeFlags.endConnecting(profileID: profile.id)
                    self.clearSyncProgress()
                    self.onStateChanged?()
                    return
                }

                // The failed sync may have reset the shared snapshot cache. If a previous profile is
                // still active, restore its remote index so Home keeps showing its real library.
                if let prev = self.dependencies.appSession.activeProfile {
                    var restored = false
                    if let prevPassword = prev.resolvedSessionCredential(from: self.dependencies.appSession) {
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
                            self.onConnectFailed?(failureProfile, error)
                        }
                        return
                    }
                }

                self.connectingProfile = nil
                self.connectTask = nil
                self.dependencies.appRuntimeFlags.endConnecting(profileID: profile.id)
                self.clearSyncProgress()
                self.onStateChanged?()
                if reportFailure {
                    self.onConnectFailed?(failureProfile, error)
                }
            }
        }
        return true
    }

    func adoptPersistedConnectionProfile(_ profile: ServerProfileRecord) {
        guard let profileID = profile.id else { return }
        let isPreparingCurrentConnection = connectingProfile?.id == profileID
        if isPreparingCurrentConnection {
            connectingProfile = profile
        }
        let session = dependencies.appSession.snapshot
        let updatesActiveProfile = session.activeProfile?.id == profileID
        if let activeProfile = session.activeProfile,
           !isPreparingCurrentConnection,
           updatesActiveProfile,
           activeProfile.remoteDestinationIdentity != profile.remoteDestinationIdentity {
            disconnect()
        }
        loadProfiles()
        dependencies.profileReachabilityService.setProfiles(
            savedProfiles,
            activeProfileID: dependencies.appSession.activeProfile?.id
        )
        onProfilesChanged?()
    }

    private static func traceConnect(
        _ stage: String,
        profile: ServerProfileRecord,
        duration: TimeInterval?,
        extra: String = ""
    ) {
        let durationText = duration.map { String(format: "%.1f", $0 * 1_000) } ?? "-"
        let suffix = extra.isEmpty ? "" : " \(extra)"
        let idText = profile.id.map(String.init) ?? "-"
        let message = "[ConnectTrace] stage=\(stage) profileID=\(idText) storage=\(profile.resolvedStorageType.rawValue) durationMs=\(durationText)\(suffix)"
        #if DEBUG
        print(message)
        #endif
    }

    private static func elapsedSeconds(since start: CFAbsoluteTime) -> TimeInterval {
        max(CFAbsoluteTimeGetCurrent() - start, 0)
    }
}
