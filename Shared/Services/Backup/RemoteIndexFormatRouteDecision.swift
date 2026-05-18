import Foundation

enum RemoteIndexSyncRoute: Equatable, Sendable {
    case v1
    case fresh
    case v2(allowPreMaterialized: Bool)
}

struct RemoteIndexFormatRouteDecision: Sendable {
    static func decide(
        inspection: RemoteFormatInspection,
        alreadyV2: Bool,
        expectV2: Bool
    ) throws -> RemoteIndexSyncRoute {
        switch inspection {
        case .unsupported(let minAppVersion):
            throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
        case .v2:
            return .v2(allowPreMaterialized: true)
        case .v2WithPendingMigrationCleanup:
            return .v2(allowPreMaterialized: false)
        case .v2WithV1Manifests:
            throw BackupCompatibilityError.requiresForegroundMigration
        case .v1:
            if alreadyV2 || expectV2 {
                throw BackupCompatibilityError.requiresForegroundMigration
            }
            return .v1
        case .fresh:
            if alreadyV2 || expectV2 {
                throw BackupCompatibilityError.damagedV2Repo
            }
            return .fresh
        }
    }
}
