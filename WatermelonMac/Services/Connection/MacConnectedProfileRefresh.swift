import Foundation

enum MacConnectedProfileRefreshMode: Equatable {
    case metadata
    case reactivation
}

enum MacConnectedProfileRefresh {
    static func activate(
        profile: ServerProfileRecord,
        credential: String,
        in appSession: AppSession
    ) {
        var sessionProfile = profile
        sessionProfile.backgroundBackupEnabled = false
        appSession.activate(
            profile: sessionProfile,
            password: credential
        )
    }

    static func mode(
        session: AppSession.Snapshot,
        updatedProfile: ServerProfileRecord,
        updatedCredential: String
    ) -> MacConnectedProfileRefreshMode {
        guard let activeProfile = session.activeProfile,
              let profileID = activeProfile.id,
              profileID == updatedProfile.id,
              activeProfile.hasSameRemoteDestination(
                  as: updatedProfile
              ),
              activeProfile.connectionParams
                == updatedProfile.connectionParams,
              activeProfile.credentialRef
                == updatedProfile.credentialRef,
              activeProfile.writerID == updatedProfile.writerID,
              session.activePassword == updatedCredential else {
            return .reactivation
        }
        return .metadata
    }

    @discardableResult
    static func apply(
        updatedProfile: ServerProfileRecord,
        updatedCredential: String,
        to appSession: AppSession
    ) -> MacConnectedProfileRefreshMode {
        let resolvedMode = mode(
            session: appSession.snapshot,
            updatedProfile: updatedProfile,
            updatedCredential: updatedCredential
        )
        guard resolvedMode == .metadata,
              let profileID = updatedProfile.id else {
            activate(
                profile: updatedProfile,
                credential: updatedCredential,
                in: appSession
            )
            return .reactivation
        }
        appSession.setActiveName(
            updatedProfile.name,
            profileID: profileID
        )
        appSession.setActiveGenerateRemoteThumbnails(
            updatedProfile.generateRemoteThumbnails,
            profileID: profileID
        )
        appSession.setActiveUploadWorkerCountMode(
            updatedProfile.uploadWorkerCountMode,
            profileID: profileID
        )
        return .metadata
    }
}
