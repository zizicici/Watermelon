import AppIntents
import Foundation

// Lets users flip a node's background-backup switch from Shortcuts automations (e.g. enable on home Wi-Fi,
// disable when leaving). Pure boolean write — no backup work runs here, so the ~30s intent budget is irrelevant.

struct BackupNodeEntity: AppEntity {
    let id: Int
    let title: String
    let subtitle: String

    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(name: LocalizedStringResource("backgroundBackup.intent.entityType", defaultValue: "Backup Node"))
    }

    // Subtitle (type + URL) disambiguates same-named nodes of different storage types.
    var displayRepresentation: DisplayRepresentation {
        DisplayRepresentation(title: "\(title)", subtitle: "\(subtitle)")
    }

    static var defaultQuery = BackupNodeQuery()
}

struct BackupNodeQuery: EntityQuery {
    func entities(for identifiers: [Int]) async throws -> [BackupNodeEntity] {
        try Self.allNodes().filter { identifiers.contains($0.id) }
    }

    func suggestedEntities() async throws -> [BackupNodeEntity] {
        try Self.allNodes()
    }

    private static func allNodes() throws -> [BackupNodeEntity] {
        let db = try DatabaseManager()
        return try db.fetchServerProfiles()
            .groupedByStorageType(excluding: [.externalVolume])
            .flatMap(\.profiles)
            .compactMap { profile in
                guard let id = profile.id else { return nil }
                return BackupNodeEntity(
                    id: Int(id),
                    title: profile.name,
                    subtitle: profile.storageProfile.displaySubtitle
                )
            }
    }
}

enum NodeBackupAction: String, AppEnum {
    case on
    case off

    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(name: LocalizedStringResource("backgroundBackup.intent.actionType", defaultValue: "Backup Action"))
    }

    static var caseDisplayRepresentations: [NodeBackupAction: DisplayRepresentation] {
        [
            .on: DisplayRepresentation(title: LocalizedStringResource("backgroundBackup.intent.action.on", defaultValue: "Turn On")),
            .off: DisplayRepresentation(title: LocalizedStringResource("backgroundBackup.intent.action.off", defaultValue: "Turn Off")),
        ]
    }
}

struct SetBackupNodeIntent: AppIntent {
    static var title = LocalizedStringResource("backgroundBackup.intent.title", defaultValue: "Set Node Background Backup")

    @Parameter(title: LocalizedStringResource("backgroundBackup.intent.nodeParam", defaultValue: "Node"))
    var node: BackupNodeEntity

    @Parameter(title: LocalizedStringResource("backgroundBackup.intent.actionParam", defaultValue: "Action"))
    var action: NodeBackupAction

    func perform() async throws -> some IntentResult & ProvidesDialog {
        let db = try DatabaseManager()
        let profileID = Int64(node.id)
        guard try db.fetchServerProfiles().contains(where: { $0.id == profileID }) else {
            throw BackupNodeIntentError(message: String(localized: "backgroundBackup.intent.result.notFound"))
        }
        let enabled: Bool
        switch action {
        case .on: enabled = true
        case .off: enabled = false
        }
        try db.setBackgroundBackupEnabled(enabled, profileID: profileID)
        await MainActor.run {
            NotificationCenter.default.post(name: .BackgroundBackupProfileChanged, object: nil)
        }
        let dialog: IntentDialog = enabled
            ? IntentDialog(LocalizedStringResource("backgroundBackup.intent.result.on", defaultValue: "Background backup turned on."))
            : IntentDialog(LocalizedStringResource("backgroundBackup.intent.result.off", defaultValue: "Background backup turned off."))
        return .result(dialog: dialog)
    }
}

struct BackupNodeIntentError: LocalizedError {
    let message: String
    var errorDescription: String? { message }
}
