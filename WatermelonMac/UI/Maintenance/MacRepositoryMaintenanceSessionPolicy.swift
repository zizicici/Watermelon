import Foundation

enum MacRepositoryMaintenanceSessionPolicy {
    static func matches(
        representedProfile: ServerProfileRecord,
        representedGeneration: UInt64,
        current: AppSession.Snapshot
    ) -> Bool {
        guard current.generation == representedGeneration,
              let activeProfile = current.activeProfile else {
            return false
        }
        return activeProfile.runtimeConnectionIdentity
                == representedProfile.runtimeConnectionIdentity
            && RemoteIndexSyncService.remoteProfileKey(activeProfile)
                == RemoteIndexSyncService.remoteProfileKey(
                    representedProfile
                )
    }

    static func shouldClose(
        representedProfile: ServerProfileRecord,
        representedGeneration: UInt64,
        current: AppSession.Snapshot,
        isBusy: Bool
    ) -> Bool {
        guard !isBusy else { return false }
        return !matches(
            representedProfile: representedProfile,
            representedGeneration: representedGeneration,
            current: current
        )
    }
}

struct MacRepositoryMaintenanceContext: Sendable {
    let profile: ServerProfileRecord
    let credential: String
    let sessionGeneration: UInt64

    static func capture(
        representedProfile: ServerProfileRecord,
        representedGeneration: UInt64,
        current: AppSession.Snapshot
    ) -> Self? {
        guard MacRepositoryMaintenanceSessionPolicy.matches(
            representedProfile: representedProfile,
            representedGeneration: representedGeneration,
            current: current
        ), let credential = resolvedCredential(
            for: representedProfile,
            current: current
        ) else {
            return nil
        }
        return Self(
            profile: representedProfile,
            credential: credential,
            sessionGeneration: representedGeneration
        )
    }

    func isCurrent(_ current: AppSession.Snapshot) -> Bool {
        guard MacRepositoryMaintenanceSessionPolicy.matches(
            representedProfile: profile,
            representedGeneration: sessionGeneration,
            current: current
        ) else {
            return false
        }
        return Self.resolvedCredential(
            for: profile,
            current: current
        ) == credential
    }

    private static func resolvedCredential(
        for profile: ServerProfileRecord,
        current: AppSession.Snapshot
    ) -> String? {
        if profile.storageProfile.requiresStoredCredential {
            return current.activePassword
        }
        return current.activePassword ?? ""
    }
}

enum MacRepositoryMaintenanceTerminalAction: Equatable {
    case stay
    case close
    case rescan
}

enum MacRepositoryMaintenanceClosePolicy {
    static func terminalAction(
        closeRequested: Bool,
        cancelledDelete: Bool
    ) -> MacRepositoryMaintenanceTerminalAction {
        if closeRequested {
            return .close
        }
        if cancelledDelete {
            return .rescan
        }
        return .stay
    }
}
