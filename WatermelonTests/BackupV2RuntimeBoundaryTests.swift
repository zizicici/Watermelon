import XCTest
@testable import Watermelon

final class BackupV2RuntimeBoundaryTests: XCTestCase {
    private let basePath = "/repo"
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!
    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }
    override func tearDownWithError() throws {
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    func testInitialMaterializeOutputBoxPeekIsNonDestructiveAndConsumeIsOneShot() async {
        let box = InitialMaterializeOutputBox(materializeOutput(repoID: "repo-box"))

        let firstPeek = await box.peek()
        let secondPeek = await box.peek()
        XCTAssertEqual(firstPeek?.repoID, "repo-box")
        XCTAssertEqual(secondPeek?.repoID, "repo-box")

        let consumed = await box.consume()
        let secondConsume = await box.consume()
        let peekAfterConsume = await box.peek()
        XCTAssertEqual(consumed?.repoID, "repo-box")
        XCTAssertNil(secondConsume)
        XCTAssertNil(peekAfterConsume)
    }
    func testV2RuntimeBuildSyncHappyPathConsumesInitialMaterializeOutputOnce() async throws {
        let fixture = try await makeBuiltRuntime()
        let services = fixture.services
        guard let preMaterialized = await services.initialMaterializeOutput.peek() else {
            XCTFail("expected cold-start materialize output")
            await services.shutdown()
            return
        }

        let remoteIndexService = RemoteIndexSyncService()
        let digest = try await remoteIndexService.syncIndex(
            client: fixture.client,
            profile: fixture.profile,
            preMaterialized: preMaterialized,
            expectV2: true,
            localRepoID: services.repoID
        )
        let consumed = await services.initialMaterializeOutput.consume()
        let peekAfterConsume = await services.initialMaterializeOutput.peek()
        let secondConsume = await services.initialMaterializeOutput.consume()
        let currentRepoIsV2 = await remoteIndexService.currentRepoIsV2()
        let materializedRepoID = await remoteIndexService.materializedRepoID()

        XCTAssertEqual(digest.totalEntryCount, 0)
        XCTAssertEqual(consumed?.repoID, services.repoID)
        XCTAssertNil(peekAfterConsume)
        XCTAssertNil(secondConsume)
        XCTAssertEqual(currentRepoIsV2, true)
        XCTAssertEqual(materializedRepoID, services.repoID)
        await services.shutdown()
    }
    func testV2RuntimeSyncIdentityMismatchDoesNotConsumeInitialMaterializeOutputOrFallbackToV1() async throws {
        let fixture = try await makeBuiltRuntime()
        let services = fixture.services
        guard let preMaterialized = await services.initialMaterializeOutput.peek() else {
            XCTFail("expected cold-start materialize output")
            await services.shutdown()
            return
        }
        let swappedRepoID = "repo-swapped"
        await fixture.client.injectFile(
            path: RepoLayout.identityFinalizationFilePath(base: basePath),
            data: try RepoIdentityFinalizationWire(
                repoID: swappedRepoID,
                formatVersion: RepoLayout.formatVersion,
                createdAtMs: 0,
                createdByWriter: "peer"
            ).encode()
        )

        let remoteIndexService = RemoteIndexSyncService()
        do {
            _ = try await remoteIndexService.syncIndex(
                client: fixture.client,
                profile: fixture.profile,
                preMaterialized: preMaterialized,
                expectV2: true,
                localRepoID: services.repoID
            )
            XCTFail("expected repoIdentityMismatch")
        } catch BackupCompatibilityError.repoIdentityMismatch {
        } catch {
            XCTFail("expected repoIdentityMismatch, got \(error)")
        }

        let retained = await services.initialMaterializeOutput.peek()
        let currentRepoIsV2 = await remoteIndexService.currentRepoIsV2()
        let materializedRepoID = await remoteIndexService.materializedRepoID()
        let snapshot = remoteIndexService.fullSnapshot()
        XCTAssertEqual(retained?.repoID, services.repoID)
        XCTAssertEqual(currentRepoIsV2, true)
        XCTAssertNil(materializedRepoID)
        XCTAssertTrue(snapshot.assets.isEmpty)
        XCTAssertTrue(snapshot.resources.isEmpty)
        XCTAssertTrue(snapshot.assetResourceLinks.isEmpty)
        await services.shutdown()
    }
    func testPrepareV2RuntimeErrorMappingDisconnectsMetadataClientForEveryBranch() async throws {
        let cases: [MappingCase] = [
            MappingCase("unsupported") { throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: "9.9.9") } assert: { error, file, line in
                guard case BackupCompatibilityError.remoteFormatUnsupported(let minApp) = error else {
                    return XCTFail("expected remoteFormatUnsupported, got \(error)", file: file, line: line)
                }
                XCTAssertEqual(minApp, "9.9.9", file: file, line: line)
            },
            MappingCase("identity mismatch") { throw BackupV2RuntimeBuildError.repoIdentityMismatch(stored: "a", observed: "b") } assert: { error, file, line in
                guard case BackupCompatibilityError.repoIdentityMismatch = error else {
                    return XCTFail("expected repoIdentityMismatch, got \(error)", file: file, line: line)
                }
            },
            MappingCase("foreground migration") { throw BackupV2RuntimeBuildError.requiresForegroundMigration } assert: { error, file, line in
                guard case BackupCompatibilityError.requiresForegroundMigration = error else {
                    return XCTFail("expected requiresForegroundMigration, got \(error)", file: file, line: line)
                }
            },
            MappingCase("format regression") { throw BackupV2RuntimeBuildError.repoFormatRegression(repoID: "repo") } assert: { error, file, line in
                guard case BackupCompatibilityError.repoFormatRegression = error else {
                    return XCTFail("expected repoFormatRegression, got \(error)", file: file, line: line)
                }
            },
            MappingCase("damaged v2") { throw BackupV2RuntimeBuildError.damagedV2Repo } assert: { error, file, line in
                guard case BackupCompatibilityError.damagedV2Repo = error else {
                    return XCTFail("expected damagedV2Repo, got \(error)", file: file, line: line)
                }
            },
            MappingCase("missing profile id") { throw BackupV2RuntimeBuildError.profileMissingID } assert: { error, file, line in
                let nsError = error as NSError
                XCTAssertEqual(nsError.domain, "BackupRunPreparation", file: file, line: line)
                XCTAssertEqual(nsError.code, -90, file: file, line: line)
            },
            MappingCase("catch all") { throw ProbeError.generic } assert: { error, file, line in
                XCTAssertEqual(error as? ProbeError, .generic, file: file, line: line)
            }
        ]

        for testCase in cases {
            let metadataClient = InMemoryRemoteStorageClient()
            try await metadataClient.connect()
            do {
                let _: Void = try await withBackupV2RuntimeBuildErrorMapping(metadataClient: metadataClient) {
                    try await testCase.build()
                }
                XCTFail("expected \(testCase.name) to throw")
            } catch {
                testCase.assert(error, #filePath, #line)
            }
            let disconnectCount = await metadataClient.disconnectCount
            XCTAssertEqual(disconnectCount, 1, testCase.name)
        }
    }
    func testProductionBackupV2RuntimeBuilderCallSitesDoNotPassRetentionRuntimeMode() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let excluded: Set<String> = [
            "Shared/Services/Repo/BackupV2RuntimeBuilder.swift",
            "Shared/Services/Repo/BackupV2RuntimeServices.swift",
            "Shared/Services/Repo/RepoRetentionRuntimeMode.swift"
        ]
        var matches: [String] = []

        for directory in ["Watermelon", "Shared", "WatermelonMac"] {
            let dirURL = root.appendingPathComponent(directory)
            guard let enumerator = FileManager.default.enumerator(at: dirURL, includingPropertiesForKeys: nil) else {
                XCTFail("could not enumerate \(directory)")
                continue
            }
            for case let fileURL as URL in enumerator where fileURL.pathExtension == "swift" {
                let relative = String(fileURL.path.dropFirst(root.path.count + 1))
                guard !excluded.contains(relative) else { continue }
                let contents = try String(contentsOf: fileURL, encoding: .utf8)
                for (index, line) in contents.split(separator: "\n", omittingEmptySubsequences: false).enumerated() {
                    if line.range(of: #"\bretentionRuntimeMode\s*:"#, options: .regularExpression) != nil {
                        matches.append("\(relative):\(index + 1):\(line.trimmingCharacters(in: .whitespaces))")
                    }
                }
            }
        }

        XCTAssertTrue(matches.isEmpty, matches.joined(separator: "\n"))
    }
    private func makeBuiltRuntime() async throws -> (
        client: InMemoryRemoteStorageClient,
        profile: ServerProfileRecord,
        services: BackupV2RuntimeServices
    ) {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let profile = try insertProfile()
        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: client,
            runMaintenanceTasks: false,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        return (client, profile, services)
    }
    private func insertProfile() throws -> ServerProfileRecord {
        let id = try TestFixtures.insertServerProfile(in: databaseManager, basePath: basePath, storageType: .webdav)
        return TestFixtures.makeServerProfile(id: id, storageType: .webdav, basePath: basePath)
    }
    private func materializeOutput(repoID: String) -> RepoMaterializer.MaterializeOutput {
        RepoMaterializer.MaterializeOutput(
            state: .empty,
            observedSeqByWriter: [:],
            coveredByMonth: [:],
            acceptedSnapshotBaselinesByMonth: [:],
            corruptedSnapshotMonths: [],
            repoID: repoID
        )
    }
    private struct MappingCase {
        let name: String
        let build: () async throws -> Void
        let assert: (Error, StaticString, UInt) -> Void

        init(
            _ name: String,
            build: @escaping () async throws -> Void,
            assert: @escaping (Error, StaticString, UInt) -> Void
        ) {
            self.name = name
            self.build = build
            self.assert = assert
        }
    }

    private enum ProbeError: Error, Equatable {
        case generic
    }
}
