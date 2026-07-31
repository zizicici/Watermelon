import Foundation

enum RemoteSyncProgressPresentation {
    static func message(for progress: RemoteSyncProgress) -> String {
        switch progress.kind {
        case .scanningRemoteIndex, .leftoverMaintenance:
            return String(localized: "home.overlay.scanningIndex")
        case .remoteIndex:
            return String.localizedStringWithFormat(
                String(localized: "home.overlay.processingMonths"),
                progress.current,
                progress.total
            )
        case .repoUpgrade(let phase):
            return repoUpgradeMessage(
                phase: phase,
                progress: progress
            )
        }
    }

    private static func repoUpgradeMessage(
        phase: RepoUpgradePhase,
        progress: RemoteSyncProgress
    ) -> String {
        switch phase {
        case .finalizing:
            return String(localized: "home.overlay.finalizingRepo")
        case .copying:
            return countedMessage(
                progress,
                format: String(
                    localized: "home.overlay.upgradingRepoMonths"
                ),
                fallback: String(
                    localized: "home.overlay.upgradingRepo"
                )
            )
        case .validating:
            return countedMessage(
                progress,
                format: String(
                    localized: "home.overlay.validatingRepoMonths"
                ),
                fallback: String(
                    localized: "home.overlay.upgradingRepo"
                )
            )
        case .cleaning:
            return countedMessage(
                progress,
                format: String(
                    localized: "home.overlay.cleaningRepoMonths"
                ),
                fallback: String(
                    localized: "home.overlay.cleaningRepo"
                )
            )
        }
    }

    private static func countedMessage(
        _ progress: RemoteSyncProgress,
        format: String,
        fallback: String
    ) -> String {
        guard progress.total > 0 else { return fallback }
        return String.localizedStringWithFormat(
            format,
            progress.current,
            progress.total
        )
    }
}
