import Foundation

struct LibraryLocalPresenceInput: Sendable {
    let localIDByFingerprint: [Data: String]
    let databaseMs: Double
    let source: String
    var restoreOrigins = RestoreOriginIndex()
    var profileKey: String? = nil
}

struct LibraryRemotePresenceInput: Sendable {
    let state: RemoteLibrarySnapshotState
    let monthCount: Int
    let isAuthoritative: Bool
    let snapshotMs: Double
}

final class LibraryPresenceInputLoader: @unchecked Sendable {
    private let hashIndexRepository: ContentHashIndexRepository
    private let coordinator: BackupCoordinator

    init(
        hashIndexRepository: ContentHashIndexRepository,
        coordinator: BackupCoordinator
    ) {
        self.hashIndexRepository = hashIndexRepository
        self.coordinator = coordinator
    }

    func loadLocal(homeSeed: HomeBrowserLocalSeed?, profileKey: String? = nil) async -> LibraryLocalPresenceInput? {
        let hashIndexRepository = hashIndexRepository
        return await withCancellableDetachedValue(priority: .userInitiated) {
            let startedAt = CFAbsoluteTimeGetCurrent()
            var map = homeSeed?.localIDByFingerprint
                ?? (try? hashIndexRepository.fetchLocalIdentifiersByFingerprint())
                ?? [:]
            let origins = RestoreOriginIndex((try? hashIndexRepository.fetchRestoreOrigins(profileKey: profileKey)) ?? [])
            let visibleIDs = homeSeed.map { Set($0.assets.map(\.localIdentifier)) }
            for (fingerprint, ids) in origins.localCandidates() where map[fingerprint] == nil {
                map[fingerprint] = ids.first { visibleIDs?.contains($0) ?? true }
            }
            guard !Task.isCancelled else { return nil }
            return LibraryLocalPresenceInput(
                localIDByFingerprint: map,
                databaseMs: (CFAbsoluteTimeGetCurrent() - startedAt) * 1_000,
                source: homeSeed == nil ? "db" : "home",
                restoreOrigins: origins,
                profileKey: profileKey
            )
        }
    }

    func loadRemote(profileKey: String?) async -> LibraryRemotePresenceInput? {
        let coordinator = coordinator
        return await withCancellableDetachedValue(priority: .userInitiated) {
            let startedAt = CFAbsoluteTimeGetCurrent()
            guard profileKey != nil else {
                return Self.disconnectedRemoteInput(
                    revision: coordinator.currentSnapshotRevision(),
                    snapshotMs: (CFAbsoluteTimeGetCurrent() - startedAt) * 1_000
                )
            }
            let captured = coordinator.currentRemoteSnapshotState(since: nil)
            guard !Task.isCancelled else { return nil }
            let authoritative = RemoteSnapshotOwnership.matches(
                ownerProfileKey: captured.profileKey,
                expectedProfileKey: profileKey
            )
            let state = authoritative
                ? captured
                : RemoteLibrarySnapshotState(
                    revision: captured.revision,
                    isFullSnapshot: captured.isFullSnapshot,
                    monthDeltas: [],
                    profileKey: captured.profileKey
                )
            return LibraryRemotePresenceInput(
                state: state,
                monthCount: captured.monthDeltas.count,
                isAuthoritative: authoritative,
                snapshotMs: (CFAbsoluteTimeGetCurrent() - startedAt) * 1_000
            )
        }
    }

    static func disconnectedRemoteInput(
        revision: UInt64,
        snapshotMs: Double = 0
    ) -> LibraryRemotePresenceInput {
        LibraryRemotePresenceInput(
            state: RemoteLibrarySnapshotState(
                revision: revision,
                isFullSnapshot: true,
                monthDeltas: [],
                profileKey: nil
            ),
            monthCount: 0,
            isAuthoritative: true,
            snapshotMs: snapshotMs
        )
    }
}
