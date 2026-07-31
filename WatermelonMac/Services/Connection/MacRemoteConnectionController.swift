import Foundation

enum MacRemoteConnectionState {
    case idle
    case connecting(ServerProfileRecord, RemoteSyncProgress?)
    case connected(ServerProfileRecord, RemoteIndexSyncDigest)
    case failed(ServerProfileRecord, String)

    var connectedProfile: ServerProfileRecord? {
        guard case .connected(let profile, _) = self else { return nil }
        return profile
    }

    var isConnecting: Bool {
        if case .connecting = self { return true }
        return false
    }
}

enum MacConnectionProfileAdoptionPolicy {
    static func canAdoptDuringConnection(
        current: ServerProfileRecord,
        updated: ServerProfileRecord
    ) -> Bool {
        guard let currentID = current.id,
              currentID == updated.id,
              current.resolvedStorageType == .sftp,
              updated.resolvedStorageType == .sftp,
              current.credentialRef == updated.credentialRef,
              let currentIdentity = current.duplicateIdentity,
              currentIdentity == updated.duplicateIdentity,
              current.sftpParams?.authMethod
                == updated.sftpParams?.authMethod else {
            return false
        }
        return true
    }
}

enum MacConnectionCredentialPromptPolicy {
    static func submittedCredential(
        from value: String?
    ) throws -> String {
        guard let value else { throw CancellationError() }
        guard !value.isEmpty else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return value
    }
}

enum MacConnectionAttemptPolicy {
    static func isCurrent(
        profileID: Int64?,
        selectedProfileID: Int64?,
        attemptEpoch: UInt64,
        currentEpoch: UInt64
    ) -> Bool {
        guard let profileID else { return false }
        return profileID == selectedProfileID
            && attemptEpoch == currentEpoch
    }
}

enum MacConnectionCredentialPersistencePolicy {
    static func accountAfterValidation(
        requiresPersistence: Bool,
        attemptedCredentialRef: String,
        liveCredentialRef: String
    ) -> String? {
        guard requiresPersistence,
              !liveCredentialRef.isEmpty,
              liveCredentialRef == attemptedCredentialRef else {
            return nil
        }
        return liveCredentialRef
    }
}

private struct MacConnectionCredentialResolution {
    let value: String
    let shouldPersist: Bool
}

private enum MacRemoteConnectionError: LocalizedError {
    case credentialUnavailable
    case profileChanged

    var errorDescription: String? {
        switch self {
        case .credentialUnavailable:
            return String(
                localized: "mac.connection.credentialUnavailable",
                defaultValue: "The saved credential is unavailable. Edit this destination and save its credential again."
            )
        case .profileChanged:
            return String(
                localized: "mac.connection.profileChanged",
                defaultValue: "The destination changed while it was connecting. Try again."
            )
        }
    }
}

private final class MacConnectionProgressSequencer: @unchecked Sendable {
    private let lock = NSLock()
    private var sequence: UInt64 = 0

    func next() -> UInt64 {
        lock.withLock {
            sequence &+= 1
            return sequence
        }
    }
}

@MainActor
final class MacRemoteConnectionController {
    private let databaseManager: DatabaseManager
    private let keychainService: KeychainService
    private let appSession: AppSession
    private let appRuntimeFlags: AppRuntimeFlags
    private let profileStore: ProfileStore
    private let connectionService: StorageProfileConnectionService
    private let remoteLibraryReadService: RemoteLibraryReadService
    private let backupCoordinator: BackupCoordinator
    private var connectTask: Task<Void, Never>?
    private var epoch: UInt64 = 0
    private var lastProgressSequence: UInt64 = 0
    private var connectingProfileID: Int64?
    private(set) var selectedProfile: ServerProfileRecord?

    private(set) var state: MacRemoteConnectionState = .idle {
        didSet {
            onChange?(state)
        }
    }

    var onChange: ((MacRemoteConnectionState) -> Void)?
    var onRemoteSnapshot: ((
        RemoteLibrarySnapshotState?,
        Bool,
        UInt64
    ) -> Void)?
    var onNeedsCredential: ((
        ServerProfileRecord
    ) async -> String?)?
    var onNeedsSFTPHostKeyTrust: ((
        ServerProfileRecord,
        SFTPHostKeyPromptPolicy.Decision,
        String
    ) async -> Bool)?
    var onConnectionFailed: ((
        ServerProfileRecord,
        String
    ) -> Void)?
    var onSelectionReverted: ((ServerProfileRecord) -> Void)?

    init(
        databaseManager: DatabaseManager,
        keychainService: KeychainService,
        appSession: AppSession,
        appRuntimeFlags: AppRuntimeFlags,
        profileStore: ProfileStore,
        connectionService: StorageProfileConnectionService,
        remoteLibraryReadService: RemoteLibraryReadService,
        backupCoordinator: BackupCoordinator
    ) {
        self.databaseManager = databaseManager
        self.keychainService = keychainService
        self.appSession = appSession
        self.appRuntimeFlags = appRuntimeFlags
        self.profileStore = profileStore
        self.connectionService = connectionService
        self.remoteLibraryReadService = remoteLibraryReadService
        self.backupCoordinator = backupCoordinator
    }

    deinit {
        connectTask?.cancel()
        appRuntimeFlags.endConnecting(profileID: connectingProfileID)
    }

    func select(profile: ServerProfileRecord?) {
        selectedProfile = profile
        #if DEBUG
        if let profile,
           ProcessInfo.processInfo.arguments.contains("--demo-connecting") {
            cancelAttempt()
            state = .connecting(profile, nil)
            return
        }
        if let profile,
           ProcessInfo.processInfo.arguments.contains("--demo-connected") {
            cancelAttempt()
            state = .connected(
                profile,
                RemoteIndexSyncDigest(
                    resourceCount: 2_480,
                    assetCount: 1_410,
                    linkCount: 2_480
                )
            )
            return
        }
        #endif
        if let profile,
           case .connecting(
               let connectingProfile,
               let progress
           ) = state,
           MacConnectionProfileAdoptionPolicy
            .canAdoptDuringConnection(
                current: connectingProfile,
                updated: profile
            ) {
            state = .connecting(profile, progress)
            return
        }
        if case .connected(let connectedProfile, let digest) = state,
           connectedProfile.id == profile?.id,
           let profile {
            guard connectedProfile.hasSameRemoteDestination(
                as: profile
            ) else {
                cancelAttempt()
                clearActiveSession()
                state = .idle
                return
            }
            let credential = (
                try? keychainService.readPassword(
                    account: profile.credentialRef
                )
            ) ?? appSession.activePassword
            if let credential {
                MacConnectedProfileRefresh.apply(
                    updatedProfile: profile,
                    updatedCredential: credential,
                    to: appSession
                )
            }
            state = .connected(profile, digest)
            return
        }
        guard state.connectedProfile?.id != profile?.id else { return }
        cancelAttempt()
        if appSession.activeProfile != nil {
            clearActiveSession()
        }
        state = .idle
    }

    func connectSelected() {
        guard let selectedProfile else { return }
        connect(profile: selectedProfile)
    }

    func attemptAutoConnect(profile: ServerProfileRecord) {
        selectedProfile = profile
        if profile.storageProfile.requiresStoredCredential,
           (try? keychainService.readPassword(
               account: profile.credentialRef
           )) == nil {
            state = .idle
            return
        }
        connect(profile: profile, reportFailure: false)
    }

    func refreshConnectedProfile(profileID: Int64) {
        guard let profile = state.connectedProfile,
              profile.id == profileID,
              selectedProfile?.id == profileID else {
            return
        }
        connect(
            profile: profile,
            reportFailure: false,
            forceReload: true
        )
    }

    func disconnect() {
        cancelAttempt()
        clearActiveSession()
        state = .idle
    }

    func connect(
        profile: ServerProfileRecord,
        reportFailure: Bool = true,
        forceReload: Bool = false
    ) {
        if !forceReload,
           case .connected(let connectedProfile, let digest) = state,
           connectedProfile.id == profile.id,
           connectedProfile.hasSameRemoteDestination(as: profile) {
            selectedProfile = profile
            state = .connected(profile, digest)
            return
        }
        let previousProfile = appSession.activeProfile
        let previousDigest: RemoteIndexSyncDigest?
        if case .connected(let connectedProfile, let digest) = state,
           connectedProfile.id == previousProfile?.id {
            previousDigest = digest
        } else {
            previousDigest = nil
        }
        let previousSnapshot: RemoteLibrarySnapshotState?
        if forceReload, let previousProfile {
            let snapshot =
                remoteLibraryReadService.currentSnapshotState()
            previousSnapshot = RemoteSnapshotOwnership.matches(
                ownerProfileKey: snapshot.profileKey,
                expectedProfileKey:
                    RemoteIndexSyncService.remoteProfileKey(
                        previousProfile
                    )
            ) ? snapshot : nil
        } else {
            previousSnapshot = nil
        }
        cancelAttempt()
        selectedProfile = profile
        guard appRuntimeFlags.tryBeginConnecting(profileID: profile.id) else {
            let message = String(
                localized: "mac.maintenance.busyMessage",
                defaultValue: "Another backup or maintenance operation is already in progress."
            )
            if let previousProfile, let previousDigest {
                selectedProfile = previousProfile
                state = .connected(previousProfile, previousDigest)
                onSelectionReverted?(previousProfile)
            } else {
                state = reportFailure
                    ? .failed(profile, message)
                    : .idle
            }
            if reportFailure {
                onConnectionFailed?(profile, message)
            }
            return
        }
        connectingProfileID = profile.id
        epoch &+= 1
        let currentEpoch = epoch
        lastProgressSequence = 0
        let sequencer = MacConnectionProgressSequencer()
        state = .connecting(profile, nil)

        connectTask = Task { [weak self] in
            guard let self else { return }
            var failureProfile = profile
            do {
                let credentialResolution = try await resolveCredential(
                    for: profile
                )
                try Task.checkCancellation()
                guard MacConnectionAttemptPolicy.isCurrent(
                    profileID: profile.id,
                    selectedProfileID: selectedProfile?.id,
                    attemptEpoch: currentEpoch,
                    currentEpoch: epoch
                ) else {
                    throw CancellationError()
                }
                let credential = credentialResolution.value
                let connectionProfile = try await connectionService
                    .prepareForConnection(
                        profile: profile,
                        confirmSFTPHostKey: { [weak self] decision, actual in
                            guard let self,
                                  let prompt = self.onNeedsSFTPHostKeyTrust else {
                                return false
                            }
                            return await prompt(profile, decision, actual)
                        }
                    )
                try Task.checkCancellation()
                if connectionProfile.connectionParams
                    != profile.connectionParams {
                    failureProfile = connectionProfile
                    adoptPersistedConnectionProfile(
                        connectionProfile,
                        epoch: currentEpoch
                    )
                }
                let digest = try await backupCoordinator.reloadRemoteIndex(
                    profile: connectionProfile,
                    password: credential,
                    onSyncProgress: { [weak self] progress in
                        let sequence = sequencer.next()
                        Task { @MainActor [weak self] in
                            self?.apply(
                                progress: progress,
                                profile: connectionProfile,
                                epoch: currentEpoch,
                                sequence: sequence
                            )
                        }
                    }
                )
                try Task.checkCancellation()
                guard currentEpoch == epoch,
                      selectedProfile?.id == profile.id else {
                    throw CancellationError()
                }
                guard let profileID = connectionProfile.id,
                      let liveProfile = try databaseManager
                        .fetchServerProfiles()
                        .first(where: { $0.id == profileID }) else {
                    throw MacRemoteConnectionError.profileChanged
                }
                let acceptedBookmarkRefresh =
                    connectionProfile.resolvedStorageType == .externalVolume
                    && databaseManager
                        .matchesAcceptedExternalBookmarkRefresh(
                            profileID: profileID,
                            previousConnectionParams:
                                connectionProfile.connectionParams,
                            currentConnectionParams:
                                liveProfile.connectionParams
                        )
                guard liveProfile.hasSameRemoteDestination(
                    as: connectionProfile
                ) || acceptedBookmarkRefresh else {
                    throw MacRemoteConnectionError.profileChanged
                }
                let credentialAccount =
                    MacConnectionCredentialPersistencePolicy
                        .accountAfterValidation(
                            requiresPersistence:
                                credentialResolution.shouldPersist,
                            attemptedCredentialRef:
                                profile.credentialRef,
                            liveCredentialRef:
                                liveProfile.credentialRef
                        )
                if credentialResolution.shouldPersist {
                    guard let credentialAccount else {
                        throw MacRemoteConnectionError.profileChanged
                    }
                    try keychainService.save(
                        password: credential,
                        account: credentialAccount
                    )
                }

                try databaseManager.setActiveServerProfileID(liveProfile.id)
                MacConnectedProfileRefresh.activate(
                    profile: liveProfile,
                    credential: credential,
                    in: appSession
                )
                selectedProfile = liveProfile
                connectTask = nil
                finishConnecting(profileID: profile.id)
                state = .connected(liveProfile, digest)
                profileStore.reload()
                onRemoteSnapshot?(
                    remoteLibraryReadService.currentSnapshotState(),
                    true,
                    appSession.snapshot.generation
                )
            } catch {
                guard currentEpoch == epoch else { return }
                connectTask = nil
                finishConnecting(profileID: profile.id)
                if error is CancellationError
                    || RemoteFaultLite.classify(error) == .cancelled {
                    if let previousProfile,
                       let previousCredential =
                        previousProfile.resolvedSessionCredential(
                            from: appSession
                        ),
                       let digest = try? await backupCoordinator
                        .reloadRemoteIndex(
                            profile: previousProfile,
                            password: previousCredential
                        ),
                       currentEpoch == epoch {
                        selectedProfile = previousProfile
                        state = .connected(
                            previousProfile,
                            digest
                        )
                        onSelectionReverted?(previousProfile)
                        onRemoteSnapshot?(
                            remoteLibraryReadService
                                .currentSnapshotState(),
                            true,
                            appSession.snapshot.generation
                        )
                    } else {
                        clearActiveSession()
                        state = .idle
                    }
                    return
                }
                let message = failureProfile
                    .userFacingStorageErrorMessage(
                    error
                )
                if forceReload,
                   let previousProfile,
                   let previousDigest,
                   previousProfile.hasSameRemoteDestination(
                       as: failureProfile
                   ) {
                    selectedProfile = previousProfile
                    state = .connected(
                        previousProfile,
                        previousDigest
                    )
                    if let previousSnapshot {
                        onRemoteSnapshot?(
                            previousSnapshot,
                            true,
                            appSession.snapshot.generation
                        )
                    }
                } else if let previousProfile,
                   !previousProfile.hasSameRemoteDestination(
                       as: failureProfile
                   ),
                   let previousCredential =
                    previousProfile.resolvedSessionCredential(
                        from: appSession
                    ),
                   let digest = try? await backupCoordinator
                    .reloadRemoteIndex(
                        profile: previousProfile,
                        password: previousCredential
                    ),
                   currentEpoch == epoch {
                    selectedProfile = previousProfile
                    state = .connected(previousProfile, digest)
                    onSelectionReverted?(previousProfile)
                    onRemoteSnapshot?(
                        remoteLibraryReadService
                            .currentSnapshotState(),
                        true,
                        appSession.snapshot.generation
                    )
                } else {
                    clearActiveSession()
                    state = reportFailure
                        ? .failed(failureProfile, message)
                        : .idle
                }
                if reportFailure {
                    onConnectionFailed?(failureProfile, message)
                }
            }
        }
    }

    private func adoptPersistedConnectionProfile(
        _ profile: ServerProfileRecord,
        epoch expectedEpoch: UInt64
    ) {
        guard expectedEpoch == epoch,
              connectingProfileID == profile.id,
              case .connecting(
                  let connectingProfile,
                  let progress
              ) = state,
              MacConnectionProfileAdoptionPolicy
                .canAdoptDuringConnection(
                    current: connectingProfile,
                    updated: profile
                ) else {
            return
        }
        selectedProfile = profile
        state = .connecting(profile, progress)
        profileStore.reload()
    }

    private func resolveCredential(
        for profile: ServerProfileRecord
    ) async throws -> MacConnectionCredentialResolution {
        guard profile.storageProfile.requiresStoredCredential else {
            return MacConnectionCredentialResolution(
                value: "",
                shouldPersist: false
            )
        }
        if let credential = try? keychainService.readPassword(
            account: profile.credentialRef
        ) {
            return MacConnectionCredentialResolution(
                value: credential,
                shouldPersist: false
            )
        }
        guard profile.storageProfile.supportsPasswordPrompt,
              let prompt = onNeedsCredential else {
            throw MacRemoteConnectionError.credentialUnavailable
        }
        let credential = try MacConnectionCredentialPromptPolicy
            .submittedCredential(
                from: await prompt(profile)
            )
        return MacConnectionCredentialResolution(
            value: credential,
            shouldPersist: true
        )
    }

    private func apply(
        progress: RemoteSyncProgress,
        profile: ServerProfileRecord,
        epoch expectedEpoch: UInt64,
        sequence: UInt64
    ) {
        guard expectedEpoch == epoch,
              sequence > lastProgressSequence,
              selectedProfile?.id == profile.id else {
            return
        }
        lastProgressSequence = sequence
        state = .connecting(profile, progress)
    }

    private func cancelAttempt() {
        epoch &+= 1
        connectTask?.cancel()
        connectTask = nil
        finishConnecting(profileID: connectingProfileID)
        lastProgressSequence = 0
    }

    private func finishConnecting(profileID: Int64?) {
        appRuntimeFlags.endConnecting(profileID: profileID)
        if connectingProfileID == profileID {
            connectingProfileID = nil
        }
    }

    private func clearActiveSession() {
        try? databaseManager.setActiveServerProfileID(nil)
        if appSession.activeProfile != nil {
            appSession.clear()
        }
        onRemoteSnapshot?(
            nil,
            false,
            appSession.snapshot.generation
        )
    }
}
