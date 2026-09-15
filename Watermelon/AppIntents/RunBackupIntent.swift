import AppIntents
import Foundation

@available(iOS 27.0, *)
extension PhotoLibraryMediaFilter: AppEnum {
    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(name: LocalizedStringResource("backupIntent.mediaType", defaultValue: "Media Type"))
    }

    static var caseDisplayRepresentations: [PhotoLibraryMediaFilter: DisplayRepresentation] {
        [
            .all: DisplayRepresentation(title: LocalizedStringResource("home.localSource.allPhotos", defaultValue: "All")),
            .photos: DisplayRepresentation(title: LocalizedStringResource("home.localSource.photos", defaultValue: "Photos")),
            .videos: DisplayRepresentation(title: LocalizedStringResource("home.localSource.videos", defaultValue: "Videos")),
        ]
    }
}

@available(iOS 27.0, *)
enum IntentBackupScope: String, AppEnum {
    case recentTwoMonths
    case allPhotos

    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(name: LocalizedStringResource("backupIntent.scope", defaultValue: "Backup Range"))
    }

    static var caseDisplayRepresentations: [IntentBackupScope: DisplayRepresentation] {
        [
            .recentTwoMonths: DisplayRepresentation(title: LocalizedStringResource("backupIntent.scope.recent", defaultValue: "Most Recent Two Months")),
            .allPhotos: DisplayRepresentation(title: LocalizedStringResource("backupIntent.scope.all", defaultValue: "Entire Photo Library")),
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
        defaultValue: "Back up photos and videos to a saved node. Requires Watermelon Pro and photo access. Uses the node’s Wi-Fi setting and the app’s iCloud originals setting. Runs independently of automatic backup switches and intervals."
    ))
    static var supportedModes: IntentModes { .background }

    @Parameter(title: LocalizedStringResource("backgroundBackup.intent.nodeParam", defaultValue: "Node"))
    var node: BackupNodeEntity

    @Parameter(title: LocalizedStringResource("backupIntent.scope", defaultValue: "Backup Range"), default: .recentTwoMonths)
    var scope: IntentBackupScope

    @Parameter(title: LocalizedStringResource("backupIntent.mediaType", defaultValue: "Media Type"), default: .all)
    var mediaType: PhotoLibraryMediaFilter

    static var parameterSummary: some ParameterSummary {
        Summary("Back up \(\.$scope) to \(\.$node)") {
            \.$mediaType
        }
    }

    func perform() async throws -> some IntentResult {
        let cancellation = BackupCancellationController()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: node.title)
        let profileID = Int64(node.id)
        let monthScope = scope.monthScope
        let mediaFilter = mediaType
        do {
            _ = try await performBackgroundTask {
                try await BackupIntentExecution.run(cancellation: cancellation) {
                    let dependencies = try DependencyContainer.makeForBackgroundTask()
                    let runner = BackgroundBackupRunner(dependencies: dependencies)
                    let result = try await runner.runOnDemand(profileID: profileID, monthScope: monthScope, mediaFilter: mediaFilter) { event in
                        await reporter.receive(event)
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
