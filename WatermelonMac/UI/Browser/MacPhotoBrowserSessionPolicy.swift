enum MacPhotoBrowserSessionPolicy {
    static func matches(
        capturedProfile: ServerProfileRecord,
        capturedGeneration: UInt64,
        current: AppSession.Snapshot
    ) -> Bool {
        guard current.generation == capturedGeneration,
              let activeProfile = current.activeProfile else {
            return false
        }
        return activeProfile.runtimeConnectionIdentity
                == capturedProfile.runtimeConnectionIdentity
            && RemoteIndexSyncService.remoteProfileKey(activeProfile)
                == RemoteIndexSyncService.remoteProfileKey(
                    capturedProfile
                )
    }
}

struct MacPhotoBrowserSessionState: Sendable {
    let profile: ServerProfileRecord?
    let credential: String?
    let generation: UInt64

    init(snapshot: AppSession.Snapshot) {
        generation = snapshot.generation
        guard let profile = snapshot.activeProfile else {
            self.profile = nil
            credential = nil
            return
        }
        self.profile = profile
        if profile.storageProfile.requiresStoredCredential {
            credential = snapshot.activePassword
        } else {
            credential = snapshot.activePassword ?? ""
        }
    }

    var hasUsableRemoteSession: Bool {
        profile != nil && credential != nil
    }
}

struct MacPhotoBrowserRemoteReadContext: Sendable {
    let profile: ServerProfileRecord
    let credential: String
    let sessionGeneration: UInt64

    func isCurrent(
        displayedSessionGeneration: UInt64?,
        current: AppSession.Snapshot
    ) -> Bool {
        displayedSessionGeneration == sessionGeneration
            && MacPhotoBrowserSessionPolicy.matches(
                capturedProfile: profile,
                capturedGeneration: sessionGeneration,
                current: current
            )
    }
}
