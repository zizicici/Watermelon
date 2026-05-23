import Foundation

enum BackupV2RepoOpenAction: Equatable, Sendable {
    case openExistingV2
    case openWithCleanupV2(ownerWriterID: String)
    case bootstrapFresh
    case migrateFromV1
    case throwUnsupported(minAppVersion: String?)
    case throwRequiresForegroundMigration
}

enum BackupV2RepoOpenPlanner {
    static func plan(
        inspection: RemoteFormatInspection,
        allowMigration: Bool
    ) -> BackupV2RepoOpenAction {
        switch inspection {
        case .unsupported(let minAppVersion):
            return .throwUnsupported(minAppVersion: minAppVersion)
        case .v2:
            return .openExistingV2
        case .v2WithPendingMigrationCleanup(_, let ownerWriterID):
            return .openWithCleanupV2(ownerWriterID: ownerWriterID)
        case .fresh:
            return .bootstrapFresh
        case .v1, .v2WithV1Manifests:
            return allowMigration ? .migrateFromV1 : .throwRequiresForegroundMigration
        }
    }
}
