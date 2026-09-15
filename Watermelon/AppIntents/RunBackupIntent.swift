import AppIntents
import Foundation

@available(iOS 27.0, *)
enum IntentBackupScope: String, AppEnum {
    case recentTwoMonths
    case allPhotos

    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(name: LocalizedStringResource("backupIntent.scope", defaultValue: "Time Range"))
    }

    static var caseDisplayRepresentations: [IntentBackupScope: DisplayRepresentation] {
        [
            .recentTwoMonths: DisplayRepresentation(title: LocalizedStringResource("backupIntent.scope.recent", defaultValue: "Most Recent Two Months")),
            .allPhotos: DisplayRepresentation(title: LocalizedStringResource("backupIntent.scope.all", defaultValue: "All Time")),
        ]
    }

    var monthScope: BackupMonthScope {
        switch self {
        case .recentTwoMonths: .recentMonths(2)
        case .allPhotos: .all
        }
    }
}

@available(iOS 27.0, *)
struct RunBackupIntent: LongRunningIntent, CancellableIntent {
    static var title = LocalizedStringResource("backupIntent.title", defaultValue: "Perform Backup")
    static var description = IntentDescription(LocalizedStringResource(
        "backupIntent.description",
        defaultValue: "Back up photos and videos to a saved node. Requires photo access and either Pro or an available Shortcuts trial run. Uses the node’s Wi-Fi setting and the app’s iCloud originals setting. Runs independently of automatic backup switches and intervals."
    ))
    static var supportedModes: IntentModes { .background }

    @Parameter(title: LocalizedStringResource("backgroundBackup.intent.nodeParam", defaultValue: "Node"))
    var node: BackupNodeEntity

    @Parameter(title: LocalizedStringResource("backupIntent.scope", defaultValue: "Time Range"), default: .recentTwoMonths)
    var scope: IntentBackupScope

    @Parameter(
        title: LocalizedStringResource("dataSource.title", defaultValue: "Data Source"),
        description: LocalizedStringResource("dataSource.intent.description", defaultValue: "Choose what to back up. Select Specific Albums to choose one or more albums for this shortcut.")
    )
    var dataSource: LocalDataSourceEntity

    @Parameter(title: LocalizedStringResource("dataSource.albums", defaultValue: "Albums"))
    var albums: [LocalAlbumEntity]?

    static var parameterSummary: some ParameterSummary {
        When(\.$dataSource, identifier: .equalTo, "albums") {
            Summary("Back up \(\.$scope) to \(\.$node)") {
                \.$dataSource
                \.$albums
            }
        } otherwise: {
            Summary("Back up \(\.$scope) to \(\.$node)") {
                \.$dataSource
            }
        }
    }

    func perform() async throws -> some IntentResult {
        try ShortcutsAccessStore.shared.checkEnabled()
        let selectedSource = try dataSource.resolve(albums: albums)
        let cancellation = BackupCancellationController()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: node.title)
        let profileID = Int64(node.id)
        let monthScope = scope.monthScope
        do {
            _ = try await performBackgroundTask {
                try await BackupIntentExecution.run(cancellation: cancellation) {
                    let result = try await ShortcutsAccess.run {
                        let dependencies = try DependencyContainer.makeForBackgroundTask()
                        let runner = BackgroundBackupRunner(dependencies: dependencies)
                        let result = try await runner.runOnDemand(profileID: profileID, monthScope: monthScope, dataSource: selectedSource) { event in
                            await reporter.receive(event)
                        }
                        try cancellation.throwIfCancelled()
                        return result
                    }
                    try cancellation.throwIfCancelled()
                    try Task.checkCancellation()
                    await reporter.complete()
                    return result
                }
            } onCancel: { _ in
                cancellation.cancel()
            }
        } catch {
            await reporter.stop()
            throw error
        }
        try cancellation.throwIfCancelled()
        try Task.checkCancellation()
        return .result()
    }
}
