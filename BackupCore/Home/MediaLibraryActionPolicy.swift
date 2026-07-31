enum MediaLibraryPresence: Sendable {
    case localOnly
    case remoteOnly
    case both
}

enum MediaLibraryActionScope: Sendable {
    case local
    case unified
}

enum MediaLibraryItemAction: Hashable, Sendable {
    case share
    case upload
    case download
    case deleteLocal
    case deleteRemote
}

struct MediaLibraryBatchItem: Sendable {
    let presence: MediaLibraryPresence
    let canDeleteLocal: Bool
    let canDeleteRemote: Bool
}

struct MediaLibraryBatchActionSummary: Equatable, Sendable {
    let showsUpload: Bool
    let showsDownload: Bool
    let localDeleteCount: Int
    let remoteDeleteCount: Int

    var showsDelete: Bool {
        localDeleteCount > 0 || remoteDeleteCount > 0
    }
}

enum MediaLibraryActionPolicy {
    static func actions(
        for presence: MediaLibraryPresence,
        scope: MediaLibraryActionScope
    ) -> [MediaLibraryItemAction] {
        switch scope {
        case .local:
            switch presence {
            case .localOnly:
                return [.share, .upload, .deleteLocal]
            case .both:
                return [.share, .deleteLocal]
            case .remoteOnly:
                return []
            }
        case .unified:
            switch presence {
            case .localOnly:
                return [.share, .upload, .deleteLocal]
            case .remoteOnly:
                return [.share, .download, .deleteRemote]
            case .both:
                return [.share, .deleteLocal, .deleteRemote]
            }
        }
    }

    static func batchSummary(
        for items: [MediaLibraryBatchItem]
    ) -> MediaLibraryBatchActionSummary {
        guard !items.isEmpty else {
            return MediaLibraryBatchActionSummary(
                showsUpload: false,
                showsDownload: false,
                localDeleteCount: 0,
                remoteDeleteCount: 0
            )
        }
        return MediaLibraryBatchActionSummary(
            showsUpload: items.allSatisfy {
                $0.presence == .localOnly
            },
            showsDownload: items.allSatisfy {
                $0.presence == .remoteOnly
            },
            localDeleteCount: items.count {
                $0.canDeleteLocal
            },
            remoteDeleteCount: items.count {
                $0.canDeleteRemote
            }
        )
    }
}
