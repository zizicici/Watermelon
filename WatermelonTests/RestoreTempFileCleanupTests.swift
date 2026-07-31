import XCTest
@testable import Watermelon

// Regression: a Media Browser download whose restore fails (or is cancelled) before the Photo Library import
// must not leak the already-downloaded full-size originals in the temp directory.
final class RestoreTempFileCleanupTests: XCTestCase {

    private func profile() -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil,
            name: "p",
            storageType: StorageType.webdav.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "host.local",
            port: 0,
            shareName: "share",
            basePath: "/p",
            username: "u",
            domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date(),
            writerID: nil
        )
    }

    // Empty resourceHash = legacy no-hash: skips integrity verification and disables content-address reuse,
    // so each resource is downloaded to its own temp file.
    private func instance(fileName: String, fileSize: Int64 = 0) -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(
            role: 1,
            slot: 0,
            resourceHash: Data(),
            fileName: fileName,
            fileSize: fileSize,
            remoteRelativePath: "2026/06/\(fileName)",
            creationDateMs: nil
        )
    }

    private func restoreTempFiles(containing token: String) -> [String] {
        let dir = FileManager.default.temporaryDirectory
        let contents = (try? FileManager.default.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil)) ?? []
        return contents.map(\.lastPathComponent).filter { $0.contains(token) }
    }

    // Resource 1 downloads to a temp file; resource 2's download throws a terminal fault before the import.
    // The already-downloaded resource-1 temp file must be removed, not orphaned in the temp directory.
    func testFailedGroupDownloadRemovesAlreadyDownloadedTempFiles() async throws {
        let token = "P02LEAK\(UUID().uuidString.replacingOccurrences(of: "-", with: ""))"
        let client = InMemoryRemoteStorageClient()
        let firstData = Data("resource-one-bytes".utf8)
        await client.enqueueDownloadData(firstData)
        await client.enqueueDownloadError(RemoteErrorFixtures.terminal)

        let service = RestoreService(makeClient: { _, _ in client })
        let items = [
            RestoreService.RestoreItemDescriptor(
                instances: [
                    instance(fileName: "\(token)_A.JPG", fileSize: Int64(firstData.count)),
                    instance(fileName: "\(token)_B.MOV")
                ],
                identity: Data([0x01])
            )
        ]

        do {
            _ = try await service.restoreItems(
                items: items,
                profile: profile(),
                password: "pw",
                onItemCompleted: { _, _, _ in }
            )
            XCTFail("expected the terminal download fault to propagate")
        } catch {
            // expected: the group's second download fails fast with a terminal fault.
        }

        let leaked = restoreTempFiles(containing: token)
        XCTAssertTrue(leaked.isEmpty, "restore left temp originals behind: \(leaked)")
    }

    func testDrainFinishesCurrentAssetAndDoesNotStartNextAsset() async throws {
        let token = "DRAIN\(UUID().uuidString.replacingOccurrences(of: "-", with: ""))"
        let client = InMemoryRemoteStorageClient()
        await client.enqueueDownloadData(Data("first".utf8))
        await client.enqueueDownloadData(Data("sidecar".utf8))
        await client.enqueueDownloadData(Data("next".utf8))
        await client.blockNextDownloadUntilOpened()
        let drain = RestoreDrainFlag()
        let recorder = RestoreDrainRecorder()
        let service = RestoreService(
            makeClient: { _, _ in client },
            importAsset: { downloaded, _ in
                await recorder.recordImport(downloaded)
                return UUID().uuidString
            }
        )
        let items = [
            RestoreService.RestoreItemDescriptor(
                instances: [
                    instance(fileName: "\(token)_first.jpg", fileSize: 5),
                    instance(fileName: "\(token)_first.mov", fileSize: 7)
                ],
                identity: Data([0x01])
            ),
            RestoreService.RestoreItemDescriptor(
                instances: [instance(fileName: "\(token)_second.jpg", fileSize: 4)],
                identity: Data([0x02])
            )
        ]

        let task = Task {
            try await service.restoreItems(
                items: items,
                profile: profile(),
                password: "pw",
                shouldDrain: { drain.value },
                onItemCompleted: { index, _, restored in
                    await recorder.recordCompletion(index: index, restored: restored != nil)
                }
            )
        }
        let enteredBlockedDownload = await client.waitUntilBlockedDownloadEntered()
        XCTAssertTrue(enteredBlockedDownload)
        drain.request()
        await client.openBlockedDownload()

        do {
            _ = try await task.value
            XCTFail("drain during the first asset must stop before the second")
        } catch is CancellationError {
        } catch {
            throw error
        }

        let attempts = await client.downloadAttemptPaths
        XCTAssertEqual(
            attempts,
            [
                "/p/2026/06/\(token)_first.jpg",
                "/p/2026/06/\(token)_first.mov"
            ]
        )
        let snapshot = await recorder.snapshot()
        XCTAssertEqual(snapshot.importCount, 1)
        XCTAssertTrue(snapshot.importSawDownloadedFile)
        XCTAssertEqual(snapshot.completedIndices, [1])
        XCTAssertEqual(snapshot.restoredCompletionCount, 1)
        XCTAssertTrue(restoreTempFiles(containing: token).isEmpty)
    }

    func testDrainAfterRecoverableDownloadFailureDoesNotEnterReconnectLoop() async throws {
        let token = "RETRYDRAIN\(UUID().uuidString.replacingOccurrences(of: "-", with: ""))"
        let client = InMemoryRemoteStorageClient()
        let drain = RestoreDrainFlag()
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)
        await client.setOnDownloadAttempt { _ in drain.request() }
        let service = RestoreService(makeClient: { _, _ in client })

        do {
            _ = try await service.restoreItems(
                items: [
                    RestoreService.RestoreItemDescriptor(
                        instances: [instance(fileName: "\(token).jpg")],
                        identity: Data([0x01])
                    )
                ],
                profile: profile(),
                password: "pw",
                shouldDrain: { drain.value },
                onItemCompleted: { _, _, _ in }
            )
            XCTFail("drain after a failed attempt must stop recovery")
        } catch is CancellationError {
        } catch {
            throw error
        }

        let downloadAttemptCount = await client.downloadAttemptPaths.count
        let disconnectCount = await client.disconnectCount
        XCTAssertEqual(downloadAttemptCount, 1)
        XCTAssertEqual(disconnectCount, 2)
        XCTAssertTrue(restoreTempFiles(containing: token).isEmpty)
    }

    func testRestoreSuccessDoesNotReturnBeforeClientDisconnects() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueDownloadData(Data("first".utf8))
        await client.blockDisconnectUntilOpened()
        let completion = RestoreDrainFlag()
        let service = RestoreService(
            makeClient: { _, _ in client },
            importAsset: { _, _ in "asset" }
        )
        let task = Task {
            defer { completion.request() }
            return try await service.restoreItems(
                items: [
                    RestoreService.RestoreItemDescriptor(
                        instances: [instance(fileName: "success.jpg", fileSize: 5)],
                        identity: Data([0x01])
                    )
                ],
                profile: profile(),
                password: "pw",
                onItemCompleted: { _, _, _ in }
            )
        }

        await client.waitUntilDisconnectEntered()
        XCTAssertFalse(completion.value)
        await client.openDisconnect()

        let restored = try await task.value
        XCTAssertEqual(restored.count, 1)
        XCTAssertTrue(completion.value)
    }

    func testRestoreFailureDoesNotThrowBeforeClientDisconnects() async {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueDownloadError(RemoteErrorFixtures.terminal)
        await client.blockDisconnectUntilOpened()
        let completion = RestoreDrainFlag()
        let service = RestoreService(makeClient: { _, _ in client })
        let task = Task {
            defer { completion.request() }
            return try await service.restoreItems(
                items: [
                    RestoreService.RestoreItemDescriptor(
                        instances: [instance(fileName: "failure.jpg")],
                        identity: Data([0x01])
                    )
                ],
                profile: profile(),
                password: "pw",
                onItemCompleted: { _, _, _ in }
            )
        }

        await client.waitUntilDisconnectEntered()
        XCTAssertFalse(completion.value)
        await client.openDisconnect()

        do {
            _ = try await task.value
            XCTFail("expected restore failure")
        } catch {
        }
        XCTAssertTrue(completion.value)
    }

    @MainActor
    func testDownloadWorkflowForwardsDrainToRestoreService() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("download-drain-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        let drainObservation = RestoreDrainFlag()
        let restoreService = DrainObservingRestoreService(observation: drainObservation)
        let helper = DownloadWorkflowHelper(
            hashIndexRepository: ContentHashIndexRepository(databaseManager: database),
            restoreService: restoreService
        )
        let remoteResource = RemoteManifestResource(
            year: 2026,
            month: 6,
            fileName: "IMG_0001.JPG",
            contentHash: Data([0x01]),
            fileSize: 1,
            resourceType: 1,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let remoteItem = RemoteAlbumItem(
            id: "item",
            assetFingerprint: Data([0x02]),
            creationDate: Date(),
            resources: [remoteResource],
            instances: [
                RemoteAssetResourceInstance(
                    role: 1,
                    slot: 0,
                    resourceHash: remoteResource.contentHash,
                    fileName: remoteResource.fileName,
                    fileSize: remoteResource.fileSize,
                    remoteRelativePath: remoteResource.remoteRelativePath,
                    creationDateMs: nil
                )
            ],
            representative: remoteResource,
            mediaKind: .photo,
            contentHashes: [remoteResource.contentHash],
            isIncomplete: false,
            missingResourceCount: 0
        )
        let control = ExecutionTerminationControl()
        control.request(.pause)

        let result = await helper.downloadItems(
            [remoteItem],
            context: DownloadWorkflowHelper.Context(profile: profile(), password: "pw"),
            incompletePolicy: .skip,
            shouldDrain: { control.shouldDrain },
            onTransferState: { _ in },
            onItemRestored: { _ in }
        )

        guard case .cancelled = result else {
            return XCTFail("download workflow must surface a restore drain as cancelled")
        }
        XCTAssertTrue(drainObservation.value)
    }
}

private final class RestoreDrainFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var requested = false

    var value: Bool {
        lock.withLock { requested }
    }

    func request() {
        lock.withLock { requested = true }
    }
}

private final class DrainObservingRestoreService: RestoreItemsServing, @unchecked Sendable {
    private let observation: RestoreDrainFlag

    init(observation: RestoreDrainFlag) {
        self.observation = observation
    }

    func restoreItems(
        items _: [RestoreService.RestoreItemDescriptor],
        profile _: ServerProfileRecord,
        password _: String,
        shouldDrain: @escaping @Sendable () -> Bool,
        onTransferState _: (nonisolated(nonsending) @Sendable (BackupTransferState) async -> Void)?,
        onItemCompleted _: nonisolated(nonsending) @Sendable (Int, Int, RestoreService.RestoredItem?) async throws -> Void
    ) async throws -> [RestoreService.RestoredItem] {
        if shouldDrain() {
            observation.request()
        }
        throw CancellationError()
    }
}

private actor RestoreDrainRecorder {
    private var importCount = 0
    private var importSawDownloadedFile = false
    private var completedIndices: [Int] = []
    private var restoredCompletionCount = 0

    func recordImport(_ downloaded: [(RemoteAssetResourceInstance, URL)]) {
        importCount += 1
        importSawDownloadedFile = downloaded.allSatisfy { FileManager.default.fileExists(atPath: $0.1.path) }
    }

    func recordCompletion(index: Int, restored: Bool) {
        completedIndices.append(index)
        if restored { restoredCompletionCount += 1 }
    }

    func snapshot() -> (importCount: Int, importSawDownloadedFile: Bool, completedIndices: [Int], restoredCompletionCount: Int) {
        (importCount, importSawDownloadedFile, completedIndices, restoredCompletionCount)
    }
}
