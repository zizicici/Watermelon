import Foundation

struct RemoteResumeCoverage: Sendable, Equatable {
    let safeToSkipAssetFingerprintsByMonth: PerMonth<Set<AssetFingerprint>>
    let healingRequiredAssetFingerprintsByMonth: PerMonth<Set<AssetFingerprint>>

    init(
        safeToSkipAssetFingerprintsByMonth: PerMonth<Set<AssetFingerprint>> = PerMonth<Set<AssetFingerprint>>(),
        healingRequiredAssetFingerprintsByMonth: PerMonth<Set<AssetFingerprint>> = PerMonth<Set<AssetFingerprint>>()
    ) {
        self.safeToSkipAssetFingerprintsByMonth = safeToSkipAssetFingerprintsByMonth
        self.healingRequiredAssetFingerprintsByMonth = healingRequiredAssetFingerprintsByMonth
    }

    func containsSafeToSkip(_ fingerprint: AssetFingerprint, in month: LibraryMonthKey) -> Bool {
        safeToSkipAssetFingerprintsByMonth.contains(fingerprint, in: month)
    }
}

/// Atomic bundle so callers can't combine stale overlay freshness with newer resume coverage.
struct RemoteViewHandle: Sendable {
    enum OverlayFreshness: Sendable, Equatable {
        case fresh
        case stale
    }

    let revision: UInt64
    let resumeCoverage: RemoteResumeCoverage
    let overlayFreshness: OverlayFreshness
    let producedAt: Date

    var safeToSkipAssetFingerprintsByMonth: PerMonth<Set<AssetFingerprint>> {
        resumeCoverage.safeToSkipAssetFingerprintsByMonth
    }

    var healingRequiredAssetFingerprintsByMonth: PerMonth<Set<AssetFingerprint>> {
        resumeCoverage.healingRequiredAssetFingerprintsByMonth
    }
}

enum RemoteViewHandleError: LocalizedError {
    case stalePhysicalPresenceOverlay
    case unknownRepositoryFormat

    var errorDescription: String? {
        switch self {
        case .stalePhysicalPresenceOverlay:
            return "Remote file verification did not finish. Resume is still paused; try again."
        case .unknownRepositoryFormat:
            return "Remote repository format was not identified. Resume is still paused; try again."
        }
    }
}
