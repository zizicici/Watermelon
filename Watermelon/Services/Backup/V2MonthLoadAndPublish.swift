import Foundation

enum V2MonthLoadAndPublish {
    static func loadAndPublishSnapshot(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        v2Services: BackupV2RuntimeServices,
        remoteIndexService: RemoteIndexSyncService,
        stepLogger: @escaping MonthManifestStepLogger
    ) async throws -> any BackupMonthStore {
        let freshHashes = await remoteIndexService.verifiedPhysicallyMissingHashes(for: month)
        let failClosedHashes = freshHashes ?? remoteIndexService.physicallyMissingHashes(for: month)
        let monthStore = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: v2Services,
            verifiedMissingHashes: failClosedHashes.isEmpty ? nil : failClosedHashes,
            overlayIsAuthoritative: freshHashes != nil,
            stepLogger: stepLogger
        )
        remoteIndexService.publishMonthSnapshot(of: monthStore, for: month)
        return monthStore
    }
}
