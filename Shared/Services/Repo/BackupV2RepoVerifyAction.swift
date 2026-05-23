import Foundation

enum BackupV2RepoVerifyAction: Equatable, Sendable {
    case throwUnsupported(minAppVersion: String?)
    case verifyMonthV2
    case throwRequiresForegroundMigration
    case verifyMonthV1
    case skipFreshRepo
    case throwDamagedV2Repo
}

enum BackupV2RepoVerifyPlanner {
    static func plan(
        inspection: RemoteFormatInspection,
        hasPriorV2Binding: Bool
    ) -> BackupV2RepoVerifyAction {
        switch inspection {
        case .unsupported(let minAppVersion):
            return .throwUnsupported(minAppVersion: minAppVersion)
        case .v2, .v2WithPendingMigrationCleanup:
            return .verifyMonthV2
        case .v2WithV1Manifests:
            return .throwRequiresForegroundMigration
        case .v1:
            return hasPriorV2Binding ? .throwRequiresForegroundMigration : .verifyMonthV1
        case .fresh:
            return hasPriorV2Binding ? .throwDamagedV2Repo : .skipFreshRepo
        }
    }
}
