import Foundation

/// Atomic bundle so callers can't combine stale overlay freshness with newer committed fingerprints.
struct RemoteViewHandle: Sendable {
    enum OverlayFreshness: Sendable, Equatable {
        case fresh
        case stale
    }

    let revision: UInt64
    let committedAssetFingerprintsByMonth: PerMonth<Set<Data>>
    let overlayFreshness: OverlayFreshness
    let producedAt: Date
}

enum RemoteViewHandleError: LocalizedError {
    case stalePhysicalPresenceOverlay

    var errorDescription: String? {
        switch self {
        case .stalePhysicalPresenceOverlay:
            return "Remote file verification did not finish. Resume is still paused; try again."
        }
    }
}
