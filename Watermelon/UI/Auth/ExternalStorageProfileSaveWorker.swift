import Foundation

nonisolated enum ExternalStorageProfileSaveWorker {
    struct Intent: Sendable {
        let editingProfile: ServerProfileRecord?
        let selectedDirectoryURL: URL?
        let name: String
    }

    enum WorkerError: Error {
        case mutationBlocked
        case profileChangedRepeatedly
    }

    struct ProfileShape: Equatable, Sendable {
        let id: Int64?
        let storageType: String
        let connectionParams: Data?
        let shareName: String

        init(_ profile: ServerProfileRecord) {
            id = profile.id
            storageType = profile.storageType
            connectionParams = profile.connectionParams
            shareName = profile.shareName
        }
    }

    struct RelevantSnapshot: Equatable, Sendable {
        let profiles: [ProfileShape]

        init(allProfiles: [ServerProfileRecord], editingProfileID: Int64?) {
            profiles = allProfiles
                .filter { profile in
                    profile.resolvedStorageType == .externalVolume || profile.id == editingProfileID
                }
                .map(ProfileShape.init)
                .sorted { ($0.id ?? Int64.min) < ($1.id ?? Int64.min) }
        }

        func matches(_ allProfiles: [ServerProfileRecord], editingProfileID: Int64?) -> Bool {
            self == RelevantSnapshot(allProfiles: allProfiles, editingProfileID: editingProfileID)
        }
    }

    private struct PreparedSnapshot: Sendable {
        let relevantSnapshot: RelevantSnapshot
        let candidateLocation: ExternalVolumeCurrentLocation
        let locationsByProfileID: [Int64: ExternalVolumeCurrentLocation]
    }

    private enum CommitAttempt {
        case retry
        case saved(ServerProfileRecord)
    }

    static func save(
        intent: Intent,
        databaseManager: DatabaseManager,
        runtimeFlags: AppRuntimeFlags
    ) throws -> ServerProfileRecord {
        try Task.checkCancellation()
        if intent.selectedDirectoryURL == nil {
            return try saveWithoutChangingLocation(
                intent: intent,
                databaseManager: databaseManager,
                runtimeFlags: runtimeFlags
            )
        }

        let selectedDirectoryURL = intent.selectedDirectoryURL!
        let bookmarkStore = SecurityScopedBookmarkStore()
        let scoped = selectedDirectoryURL.startAccessingSecurityScopedResource()
        defer {
            if scoped {
                selectedDirectoryURL.stopAccessingSecurityScopedResource()
            }
        }
        let bookmarkData = try bookmarkStore.makeBookmarkData(for: selectedDirectoryURL)
        try Task.checkCancellation()
        let displayPath = selectedDirectoryURL.path

        for _ in 0 ..< 3 {
            try Task.checkCancellation()
            let profiles = try databaseManager.fetchServerProfiles()
            try Task.checkCancellation()
            let prepared = try prepareSnapshot(
                profiles: profiles,
                editingProfileID: intent.editingProfile?.id,
                bookmarkData: bookmarkData,
                displayPath: displayPath,
                bookmarkStore: bookmarkStore
            )
            try Task.checkCancellation()

            let leasedAttempt = try runtimeFlags.withProfileMutationLease(
                profileID: intent.editingProfile?.id
            ) {
                try Task.checkCancellation()
                let liveProfiles = try databaseManager.fetchServerProfiles()
                try Task.checkCancellation()
                guard prepared.relevantSnapshot.matches(
                    liveProfiles,
                    editingProfileID: intent.editingProfile?.id
                ) else {
                    return CommitAttempt.retry
                }
                let liveEditingProfile = try requireLiveEditingProfile(
                    from: liveProfiles,
                    snapshot: intent.editingProfile
                )
                try Task.checkCancellation()
                let otherLocations = prepared.locationsByProfileID.compactMap { profileID, location in
                    profileID == intent.editingProfile?.id ? nil : location
                }
                if ExternalVolumeLocationPolicy.containsDuplicate(
                    candidate: prepared.candidateLocation,
                    existingLocations: otherLocations
                ) {
                    throw NSError(
                        domain: "AddExternalStorage",
                        code: 2,
                        userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.external.duplicateDir")]
                    )
                }
                let existingLocation = intent.editingProfile?.id.flatMap {
                    prepared.locationsByProfileID[$0]
                }
                let shareName = ExternalVolumeLocationPolicy.locationToken(
                    existingToken: liveEditingProfile?.shareName,
                    selectedNewLocation: true,
                    existingLocation: existingLocation,
                    candidateLocation: prepared.candidateLocation,
                    makeToken: { "external-\(UUID().uuidString)" }
                )
                var profile = try makeProfile(
                    baseProfile: liveEditingProfile,
                    name: intent.name,
                    bookmarkData: bookmarkData,
                    displayPath: displayPath,
                    shareName: shareName
                )
                try Task.checkCancellation()
                try databaseManager.saveConnectionProfile(
                    &profile,
                    editingProfileID: intent.editingProfile?.id
                )
                return CommitAttempt.saved(profile)
            }
            guard let leasedAttempt else { throw WorkerError.mutationBlocked }
            switch leasedAttempt {
            case .retry:
                continue
            case .saved(let profile):
                return profile
            }
        }
        throw WorkerError.profileChangedRepeatedly
    }

    private static func saveWithoutChangingLocation(
        intent: Intent,
        databaseManager: DatabaseManager,
        runtimeFlags: AppRuntimeFlags
    ) throws -> ServerProfileRecord {
        guard intent.editingProfile?.id != nil else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let leasedProfile = try runtimeFlags.withProfileMutationLease(profileID: intent.editingProfile?.id) {
            try Task.checkCancellation()
            let liveProfile = try requireLiveEditingProfile(
                from: databaseManager.fetchServerProfiles(),
                snapshot: intent.editingProfile
            )
            guard let liveProfile,
                  let params = liveProfile.externalVolumeParams else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            var profile = try makeProfile(
                baseProfile: liveProfile,
                name: intent.name,
                bookmarkData: params.rootBookmarkData,
                displayPath: params.displayPath,
                shareName: liveProfile.shareName
            )
            try Task.checkCancellation()
            try databaseManager.saveConnectionProfile(
                &profile,
                editingProfileID: liveProfile.id
            )
            return profile
        }
        guard let leasedProfile else { throw WorkerError.mutationBlocked }
        return leasedProfile
    }

    private static func prepareSnapshot(
        profiles: [ServerProfileRecord],
        editingProfileID: Int64?,
        bookmarkData: Data,
        displayPath: String,
        bookmarkStore: SecurityScopedBookmarkStore
    ) throws -> PreparedSnapshot {
        guard let fallbackLocation = makeFallbackLocation(displayPath: displayPath) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let candidateLocation = (try? bookmarkStore.currentLocation(for: bookmarkData)) ?? fallbackLocation
        try Task.checkCancellation()

        var locationsByProfileID: [Int64: ExternalVolumeCurrentLocation] = [:]
        for profile in profiles where profile.resolvedStorageType == .externalVolume {
            guard let profileID = profile.id,
                  let params = profile.externalVolumeParams else { continue }
            let location: ExternalVolumeCurrentLocation?
            do {
                location = try bookmarkStore.currentLocation(for: params.rootBookmarkData)
            } catch {
                location = makeFallbackLocation(displayPath: params.displayPath)
            }
            try Task.checkCancellation()
            if let location {
                locationsByProfileID[profileID] = location
            }
        }
        return PreparedSnapshot(
            relevantSnapshot: RelevantSnapshot(
                allProfiles: profiles,
                editingProfileID: editingProfileID
            ),
            candidateLocation: candidateLocation,
            locationsByProfileID: locationsByProfileID
        )
    }

    private static func makeFallbackLocation(displayPath: String) -> ExternalVolumeCurrentLocation? {
        guard !displayPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return nil }
        return ExternalVolumeCurrentLocation(
            fullIdentity: nil,
            volumePathIdentity: nil,
            standardizedURL: URL(fileURLWithPath: displayPath).standardizedFileURL
        )
    }

    private static func requireLiveEditingProfile(
        from profiles: [ServerProfileRecord],
        snapshot: ServerProfileRecord?
    ) throws -> ServerProfileRecord? {
        guard let snapshot else { return nil }
        guard let profileID = snapshot.id,
              let profile = profiles.first(where: { $0.id == profileID }),
              profile.resolvedStorageType == .externalVolume else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return profile
    }

    private static func makeProfile(
        baseProfile: ServerProfileRecord?,
        name: String,
        bookmarkData: Data,
        displayPath: String,
        shareName: String
    ) throws -> ServerProfileRecord {
        let params = ExternalVolumeConnectionParams(
            rootBookmarkData: bookmarkData,
            displayPath: displayPath
        )
        let encodedParams = try ServerProfileRecord.encodedConnectionParams(params)
        let profileName = baseProfile?.name
            ?? (name.isEmpty ? URL(fileURLWithPath: displayPath).lastPathComponent : name)
        return ServerProfileRecord(
            id: baseProfile?.id,
            name: profileName,
            storageType: StorageType.externalVolume.rawValue,
            connectionParams: encodedParams,
            sortOrder: baseProfile?.sortOrder ?? 0,
            host: "external",
            port: 0,
            shareName: shareName,
            basePath: "/",
            username: "local",
            domain: nil,
            credentialRef: baseProfile?.credentialRef ?? "external:\(UUID().uuidString)",
            backgroundBackupEnabled: baseProfile?.backgroundBackupEnabled ?? false,
            backgroundBackupMinIntervalMinutes: baseProfile?.backgroundBackupMinIntervalMinutes ?? BackgroundBackupInterval.default.minutes,
            backgroundBackupRequiresWiFi: baseProfile?.backgroundBackupRequiresWiFi ?? true,
            generateRemoteThumbnails: baseProfile?.generateRemoteThumbnails ?? false,
            createdAt: baseProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )
    }
}
