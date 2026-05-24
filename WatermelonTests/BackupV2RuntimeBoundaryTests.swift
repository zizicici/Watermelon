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
        let swappedRepoID = "99999999-9999-9999-9999-999999999999"
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
    func testRuntimeOpenFailureClassificationTable() {
        let cases: [(String, Error, BackupV2RuntimeOpenFailureKind)] = [
            ("unsupported", BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: "9.9.9"), .unsupportedRemoteFormat(minAppVersion: "9.9.9")),
            ("identity mismatch", BackupV2RuntimeBuildError.repoIdentityMismatch(stored: "stored", observed: "observed"), .repoIdentityMismatch),
            ("foreground migration", BackupV2RuntimeBuildError.requiresForegroundMigration, .requiresForegroundMigration),
            ("format regression", BackupV2RuntimeBuildError.repoFormatRegression(repoID: "repo"), .repoFormatRegression),
            ("damaged v2", BackupV2RuntimeBuildError.damagedV2Repo, .damagedV2Repo),
            ("missing profile id", BackupV2RuntimeBuildError.profileMissingID, .profileMissingID),
            ("cancellation", CancellationError(), .cancellation),
            (
                "wrapped cancellation",
                RemoteStorageClientError.underlying(NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)),
                .cancellation
            ),
            ("transient", RemoteStorageClientError.notConnected, .transientRemoteFailure),
            ("other", ProbeError.generic, .other)
        ]

        for (name, error, expected) in cases {
            let failure = BackupV2RuntimeOpenErrorMapping.classifyBuildFailure(error)
            XCTAssertEqual(failure.kind, expected, name)
        }

        let identity = BackupV2RuntimeBuildError.repoIdentityMismatch(stored: "stored", observed: "observed")
        let identityFailure = BackupV2RuntimeOpenErrorMapping.classifyBuildFailure(identity)
        guard case BackupV2RuntimeBuildError.repoIdentityMismatch(let stored, let observed) = identityFailure.originalError else {
            return XCTFail("expected original identity mismatch to retain associated values")
        }
        XCTAssertEqual(stored, "stored")
        XCTAssertEqual(observed, "observed")
    }

    func testRuntimeOpenCompatibilityMappingTable() {
        let cases: [(String, Error, (Error, StaticString, UInt) -> Void)] = [
            ("unsupported", BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: "9.9.9"), { error, file, line in
                guard case BackupCompatibilityError.remoteFormatUnsupported(let minApp) = error else {
                    return XCTFail("expected remoteFormatUnsupported, got \(error)", file: file, line: line)
                }
                XCTAssertEqual(minApp, "9.9.9", file: file, line: line)
            }),
            ("identity mismatch", BackupV2RuntimeBuildError.repoIdentityMismatch(stored: "a", observed: "b"), { error, file, line in
                guard case BackupCompatibilityError.repoIdentityMismatch = error else {
                    return XCTFail("expected repoIdentityMismatch, got \(error)", file: file, line: line)
                }
            }),
            ("foreground migration", BackupV2RuntimeBuildError.requiresForegroundMigration, { error, file, line in
                guard case BackupCompatibilityError.requiresForegroundMigration = error else {
                    return XCTFail("expected requiresForegroundMigration, got \(error)", file: file, line: line)
                }
            }),
            ("format regression", BackupV2RuntimeBuildError.repoFormatRegression(repoID: "repo"), { error, file, line in
                guard case BackupCompatibilityError.repoFormatRegression = error else {
                    return XCTFail("expected repoFormatRegression, got \(error)", file: file, line: line)
                }
            }),
            ("damaged v2", BackupV2RuntimeBuildError.damagedV2Repo, { error, file, line in
                guard case BackupCompatibilityError.damagedV2Repo = error else {
                    return XCTFail("expected damagedV2Repo, got \(error)", file: file, line: line)
                }
            }),
            ("missing profile id", BackupV2RuntimeBuildError.profileMissingID, { error, file, line in
                let nsError = error as NSError
                XCTAssertEqual(nsError.domain, "BackupRunPreparation", file: file, line: line)
                XCTAssertEqual(nsError.code, -90, file: file, line: line)
            }),
            ("cancellation", CancellationError(), { error, file, line in
                XCTAssertTrue(error is CancellationError, "expected CancellationError, got \(error)", file: file, line: line)
            }),
            ("transient", RemoteStorageClientError.notConnected, { error, file, line in
                guard case RemoteStorageClientError.notConnected = error else {
                    return XCTFail("expected original transient error, got \(error)", file: file, line: line)
                }
            }),
            ("catch all", ProbeError.generic, { error, file, line in
                XCTAssertEqual(error as? ProbeError, .generic, file: file, line: line)
            })
        ]

        for (_, input, assert) in cases {
            let failure = BackupV2RuntimeOpenErrorMapping.classifyBuildFailure(input)
            let mapped = BackupV2RuntimeOpenErrorMapping.compatibilityError(for: failure)
            assert(mapped, #filePath, #line)
        }
    }

    func testRuntimeOpenCompatibilityMapperDisconnectsOwnedMetadataClientForEveryBranch() async throws {
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
            MappingCase("cancellation") { throw CancellationError() } assert: { error, file, line in
                XCTAssertTrue(error is CancellationError, "expected CancellationError, got \(error)", file: file, line: line)
            },
            MappingCase("catch all") { throw ProbeError.generic } assert: { error, file, line in
                XCTAssertEqual(error as? ProbeError, .generic, file: file, line: line)
            }
        ]

        for testCase in cases {
            let metadataClient = InMemoryRemoteStorageClient()
            try await metadataClient.connect()
            do {
                let _: Void = try await BackupV2RuntimeOpenErrorMapping.withCompatibilityMapping(
                    metadataClient: metadataClient,
                    disconnectOnError: true
                ) {
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

    func testRuntimeOpenCompatibilityMapperDoesNotDisconnectBorrowedMetadataClient() async throws {
        let cases: [MappingCase] = [
            MappingCase("unsupported") { throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: nil) } assert: { _, _, _ in },
            MappingCase("identity mismatch") { throw BackupV2RuntimeBuildError.repoIdentityMismatch(stored: "a", observed: "b") } assert: { _, _, _ in },
            MappingCase("foreground migration") { throw BackupV2RuntimeBuildError.requiresForegroundMigration } assert: { _, _, _ in },
            MappingCase("format regression") { throw BackupV2RuntimeBuildError.repoFormatRegression(repoID: "repo") } assert: { _, _, _ in },
            MappingCase("damaged v2") { throw BackupV2RuntimeBuildError.damagedV2Repo } assert: { _, _, _ in },
            MappingCase("missing profile id") { throw BackupV2RuntimeBuildError.profileMissingID } assert: { _, _, _ in },
            MappingCase("catch all") { throw ProbeError.generic } assert: { _, _, _ in }
        ]

        for testCase in cases {
            let metadataClient = InMemoryRemoteStorageClient()
            try await metadataClient.connect()
            do {
                let _: Void = try await BackupV2RuntimeOpenErrorMapping.withCompatibilityMapping(
                    metadataClient: metadataClient,
                    disconnectOnError: false
                ) {
                    try await testCase.build()
                }
                XCTFail("expected \(testCase.name) to throw")
            } catch {
            }
            let disconnectCount = await metadataClient.disconnectCount
            XCTAssertEqual(disconnectCount, 0, testCase.name)
        }
    }

    func testRuntimeOpenCompatibilityMapperSuccessDoesNotDisconnect() async throws {
        let metadataClient = InMemoryRemoteStorageClient()
        try await metadataClient.connect()
        let value = try await BackupV2RuntimeOpenErrorMapping.withCompatibilityMapping(
            metadataClient: metadataClient,
            disconnectOnError: true
        ) {
            42
        }
        let disconnectCount = await metadataClient.disconnectCount
        XCTAssertEqual(value, 42)
        XCTAssertEqual(disconnectCount, 0)
    }

    func testBackgroundRuntimeOpenFailureDispositionTable() {
        let cases: [(BackupV2RuntimeOpenFailureKind, BackgroundRuntimeOpenFailureDisposition)] = [
            (.requiresForegroundMigration, .skippedForegroundMigration),
            (.unsupportedRemoteFormat(minAppVersion: "9.9.9"), .failedUnsupportedRemoteFormat(minAppVersion: "9.9.9")),
            (.repoIdentityMismatch, .failedRepoIdentityMismatch),
            (.repoFormatRegression, .failedRepoFormatRegression),
            (.damagedV2Repo, .failedDamagedV2Repo),
            (.profileMissingID, .failedProfileMissingID),
            (.cancellation, .cancelled),
            (.transientRemoteFailure, .failedTransientRemoteFailure),
            (.other, .skippedOther)
        ]

        for (kind, expected) in cases {
            XCTAssertEqual(BackgroundBackupRunner.runtimeOpenFailureDisposition(kind), expected)
        }
    }
    func testProductionBackupV2RuntimeBuilderCallSitesDoNotDisableMaintenanceExceptVerifyMonth() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let excluded: Set<String> = [
            "Shared/Services/Repo/BackupV2RuntimeBuilder.swift",
            "Shared/Services/Repo/BackupV2RuntimeServices.swift",
            "Shared/Services/Repo/RepoRetentionRuntimeMode.swift",
            "Shared/Services/Repo/RepoMaintenanceRuntime.swift"
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
                    if line.contains("maintenanceStartupMode: .disabled") {
                        let allowed = relative == "Shared/Services/Repo/BackupV2RuntimeLease.swift"
                            && line.contains(".verifyMonthTombstoneApply")
                        if !allowed {
                            matches.append("\(relative):\(index + 1):\(line.trimmingCharacters(in: .whitespaces))")
                        }
                    }
                    if line.contains("runMaintenanceTasks") {
                        matches.append("\(relative):\(index + 1):\(line.trimmingCharacters(in: .whitespaces))")
                    }
                }
            }
        }

        XCTAssertTrue(matches.isEmpty, matches.joined(separator: "\n"))
    }

    func testNormalizeOpenErrorPreservesRepoIdentityMismatchPayload() {
        let input = BackupCompatibilityError.repoIdentityMismatch(stored: "A", observed: "B")
        let normalized = BackupV2RuntimeOpenErrorMapping.normalizeOpenError(input)
        guard case BackupV2RuntimeBuildError.repoIdentityMismatch(let stored, let observed) = normalized else {
            return XCTFail("expected repoIdentityMismatch, got \(normalized)")
        }
        XCTAssertEqual(stored, "A")
        XCTAssertEqual(observed, "B")
    }

    func testNormalizeOpenErrorRepoIdentityMismatchWithNilPayloadFallsBackToEmptyStrings() {
        let input = BackupCompatibilityError.repoIdentityMismatch(stored: nil, observed: nil)
        let normalized = BackupV2RuntimeOpenErrorMapping.normalizeOpenError(input)
        guard case BackupV2RuntimeBuildError.repoIdentityMismatch(let stored, let observed) = normalized else {
            return XCTFail("expected repoIdentityMismatch, got \(normalized)")
        }
        XCTAssertEqual(stored, "")
        XCTAssertEqual(observed, "")
    }

    func testNormalizeOpenErrorPreservesRepoFormatRegressionPayload() {
        let input = BackupCompatibilityError.repoFormatRegression(repoID: "repo-abc")
        let normalized = BackupV2RuntimeOpenErrorMapping.normalizeOpenError(input)
        guard case BackupV2RuntimeBuildError.repoFormatRegression(let repoID) = normalized else {
            return XCTFail("expected repoFormatRegression, got \(normalized)")
        }
        XCTAssertEqual(repoID, "repo-abc")
    }

    func testCompatibilityErrorRoundTripsRepoIdentityMismatchPayload() {
        let build = BackupV2RuntimeBuildError.repoIdentityMismatch(stored: "X", observed: "Y")
        let failure = BackupV2RuntimeOpenErrorMapping.classifyBuildFailure(build)
        let mapped = BackupV2RuntimeOpenErrorMapping.compatibilityError(for: failure)
        guard case BackupCompatibilityError.repoIdentityMismatch(let stored, let observed) = mapped else {
            return XCTFail("expected repoIdentityMismatch, got \(mapped)")
        }
        XCTAssertEqual(stored, "X")
        XCTAssertEqual(observed, "Y")
    }

    func testCompatibilityErrorRepoIdentityMismatchEmptyPayloadBecomesNil() {
        let build = BackupV2RuntimeBuildError.repoIdentityMismatch(stored: "", observed: "")
        let failure = BackupV2RuntimeOpenErrorMapping.classifyBuildFailure(build)
        let mapped = BackupV2RuntimeOpenErrorMapping.compatibilityError(for: failure)
        guard case BackupCompatibilityError.repoIdentityMismatch(let stored, let observed) = mapped else {
            return XCTFail("expected repoIdentityMismatch, got \(mapped)")
        }
        XCTAssertNil(stored)
        XCTAssertNil(observed)
    }

    func testCompatibilityErrorRoundTripsRepoFormatRegressionPayload() {
        let build = BackupV2RuntimeBuildError.repoFormatRegression(repoID: "repo-Z")
        let failure = BackupV2RuntimeOpenErrorMapping.classifyBuildFailure(build)
        let mapped = BackupV2RuntimeOpenErrorMapping.compatibilityError(for: failure)
        guard case BackupCompatibilityError.repoFormatRegression(let repoID) = mapped else {
            return XCTFail("expected repoFormatRegression, got \(mapped)")
        }
        XCTAssertEqual(repoID, "repo-Z")
    }

    func testTranslateBootstrapErrorIOFailureExternalStorageUnavailablePassesThrough() {
        let input = RepoBootstrap.BootstrapError.ioFailure(RemoteStorageClientError.externalStorageUnavailable)
        let translated = BackupV2RuntimeOpenErrorMapping.translate(bootstrapError: input)
        XCTAssertTrue(
            RemoteStorageClientError.isLikelyExternalStorageUnavailable(translated),
            "expected external-volume passthrough, got \(translated)"
        )
    }

    func testTranslateBootstrapErrorIOFailureCancellationReturnsCancellation() {
        let input = RepoBootstrap.BootstrapError.ioFailure(CancellationError())
        let translated = BackupV2RuntimeOpenErrorMapping.translate(bootstrapError: input)
        XCTAssertTrue(translated is CancellationError, "expected CancellationError, got \(translated)")
    }

    func testTranslateBootstrapErrorIOFailureUnknownUnderlyingReturnsDamagedV2Repo() {
        let underlying = NSError(domain: "Synthetic", code: 99)
        let input = RepoBootstrap.BootstrapError.ioFailure(underlying)
        let translated = BackupV2RuntimeOpenErrorMapping.translate(bootstrapError: input)
        guard case BackupV2RuntimeBuildError.damagedV2Repo = translated else {
            return XCTFail("expected damagedV2Repo, got \(translated)")
        }
    }

    func testTranslateBootstrapErrorFutureFormatVersionReturnsUnsupportedRemoteFormat() {
        let input = RepoBootstrap.BootstrapError.futureFormatVersion(minAppVersion: "99.9")
        let translated = BackupV2RuntimeOpenErrorMapping.translate(bootstrapError: input)
        guard case BackupV2RuntimeBuildError.unsupportedRemoteFormat(let minAppVersion) = translated else {
            return XCTFail("expected unsupportedRemoteFormat, got \(translated)")
        }
        XCTAssertEqual(minAppVersion, "99.9")
    }

    func testTranslateVersionConflictHigherFormatReturnsUnsupportedRemoteFormat() {
        let input = RepoBootstrap.VersionConflict.higherFormatVersion(remote: 5, local: 2, minAppVersion: "11.0")
        let translated = BackupV2RuntimeOpenErrorMapping.translate(versionConflict: input)
        guard case BackupV2RuntimeBuildError.unsupportedRemoteFormat(let minAppVersion) = translated else {
            return XCTFail("expected unsupportedRemoteFormat, got \(translated)")
        }
        XCTAssertEqual(minAppVersion, "11.0")
    }

    func testTranslateVersionConflictMismatchedFormatReturnsUnsupportedRemoteFormat() {
        let input = RepoBootstrap.VersionConflict.mismatchedFormatVersion(remote: 2, local: 3, minAppVersion: nil)
        let translated = BackupV2RuntimeOpenErrorMapping.translate(versionConflict: input)
        guard case BackupV2RuntimeBuildError.unsupportedRemoteFormat(let minAppVersion) = translated else {
            return XCTFail("expected unsupportedRemoteFormat, got \(translated)")
        }
        XCTAssertNil(minAppVersion)
    }

    func testTranslateVersionConflictUnreadableExternalStorageUnavailablePassesThrough() {
        let input = RepoBootstrap.VersionConflict.unreadable(RemoteStorageClientError.externalStorageUnavailable)
        let translated = BackupV2RuntimeOpenErrorMapping.translate(versionConflict: input)
        XCTAssertTrue(
            RemoteStorageClientError.isLikelyExternalStorageUnavailable(translated),
            "expected external-volume passthrough, got \(translated)"
        )
    }

    func testTranslateVersionConflictUnreadableCancellationReturnsCancellation() {
        let input = RepoBootstrap.VersionConflict.unreadable(CancellationError())
        let translated = BackupV2RuntimeOpenErrorMapping.translate(versionConflict: input)
        XCTAssertTrue(translated is CancellationError, "expected CancellationError, got \(translated)")
    }

    func testTranslateVersionConflictUnreadableNilUnderlyingReturnsDamagedV2Repo() {
        let input = RepoBootstrap.VersionConflict.unreadable(nil)
        let translated = BackupV2RuntimeOpenErrorMapping.translate(versionConflict: input)
        guard case BackupV2RuntimeBuildError.damagedV2Repo = translated else {
            return XCTFail("expected damagedV2Repo, got \(translated)")
        }
    }

    func testTranslateVersionConflictUnreadableUnknownUnderlyingReturnsDamagedV2Repo() {
        let underlying = NSError(domain: "Synthetic", code: 77)
        let input = RepoBootstrap.VersionConflict.unreadable(underlying)
        let translated = BackupV2RuntimeOpenErrorMapping.translate(versionConflict: input)
        guard case BackupV2RuntimeBuildError.damagedV2Repo = translated else {
            return XCTFail("expected damagedV2Repo, got \(translated)")
        }
    }

    func testTranslateToCompatibilityErrorBootstrapDamagedReturnsCompatibilityDamagedV2Repo() {
        let input = RepoBootstrap.BootstrapError.ioFailure(NSError(domain: "Synthetic", code: 1))
        let translated = BackupV2RuntimeOpenErrorMapping.translateToCompatibilityError(bootstrapError: input)
        guard case BackupCompatibilityError.damagedV2Repo = translated else {
            return XCTFail("expected damagedV2Repo, got \(translated)")
        }
    }

    func testTranslateToCompatibilityErrorVersionConflictUnsupportedReturnsCompatibilityUnsupported() {
        let input = RepoBootstrap.VersionConflict.higherFormatVersion(remote: 9, local: 2, minAppVersion: "7.0")
        let translated = BackupV2RuntimeOpenErrorMapping.translateToCompatibilityError(versionConflict: input)
        guard case BackupCompatibilityError.remoteFormatUnsupported(let minAppVersion) = translated else {
            return XCTFail("expected remoteFormatUnsupported, got \(translated)")
        }
        XCTAssertEqual(minAppVersion, "7.0")
    }

    func testTranslateToCompatibilityErrorPreservesExternalStoragePassthrough() {
        let input = RepoBootstrap.BootstrapError.ioFailure(RemoteStorageClientError.externalStorageUnavailable)
        let translated = BackupV2RuntimeOpenErrorMapping.translateToCompatibilityError(bootstrapError: input)
        XCTAssertTrue(
            RemoteStorageClientError.isLikelyExternalStorageUnavailable(translated),
            "expected external-volume passthrough, got \(translated)"
        )
    }

    func testRemoteIndexV2SyncEnginePreMaterializedMismatchPopulatesPayload() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        let preMaterialized = try await builder.materialize()

        do {
            _ = try await RemoteIndexV2SyncEngine().materialize(
                client: builder.client,
                basePath: basePath,
                preMaterialized: preMaterialized,
                localRepoID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            )
            XCTFail("expected repoIdentityMismatch")
        } catch let BackupCompatibilityError.repoIdentityMismatch(stored, observed) {
            XCTAssertEqual(stored, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
            XCTAssertEqual(observed, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        }
    }

    func testRemoteIndexV2SyncEngineLiveSwapMismatchPopulatesPayload() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        let preMaterialized = try await builder.materialize()
        try await TestFixtures.injectRepoJSON(builder.client, basePath: basePath, repoID: "cccccccc-cccc-cccc-cccc-cccccccccccc")

        do {
            _ = try await RemoteIndexV2SyncEngine().materialize(
                client: builder.client,
                basePath: basePath,
                preMaterialized: preMaterialized,
                localRepoID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
            )
            XCTFail("expected repoIdentityMismatch after live swap")
        } catch let BackupCompatibilityError.repoIdentityMismatch(stored, observed) {
            XCTAssertEqual(stored, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
            XCTAssertEqual(observed, "cccccccc-cccc-cccc-cccc-cccccccccccc")
        }
    }

    func testRemoteIndexV2SyncEngineNoPreMaterializedMismatchPopulatesPayload() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

        do {
            _ = try await RemoteIndexV2SyncEngine().materialize(
                client: builder.client,
                basePath: basePath,
                preMaterialized: nil,
                localRepoID: "dddddddd-dddd-dddd-dddd-dddddddddddd"
            )
            XCTFail("expected repoIdentityMismatch")
        } catch let BackupCompatibilityError.repoIdentityMismatch(stored, observed) {
            XCTAssertEqual(stored, "dddddddd-dddd-dddd-dddd-dddddddddddd")
            XCTAssertEqual(observed, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        }
    }

    func testProductionRuntimeOpenErrorMappingUsesSharedAuthority() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let preparation = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Services/Backup/BackupRunPreparation.swift"),
            encoding: .utf8
        )
        let background = try String(
            contentsOf: root.appendingPathComponent("Watermelon/Services/Backup/BackgroundBackupRunner.swift"),
            encoding: .utf8
        )
        let repoOpen = try String(
            contentsOf: root.appendingPathComponent("Shared/Services/Repo/BackupV2RepoOpenService.swift"),
            encoding: .utf8
        )
        let lease = try String(
            contentsOf: root.appendingPathComponent("Shared/Services/Repo/BackupV2RuntimeLease.swift"),
            encoding: .utf8
        )

        // Lease absorbs the runtime build/open boundary. The verify pre-build
        // bootstrap-error translation stays in BackupRunPreparation.swift; that
        // path is outside the lease scope (M15 territory) and uses a different
        // helper, so it is not banned here.
        XCTAssertFalse(preparation.contains("BackupV2RuntimeOpenErrorMapping.withCompatibilityMapping"))
        XCTAssertFalse(preparation.contains("BackupV2RuntimeOpenErrorMapping.classifyBuildFailure"))
        XCTAssertFalse(preparation.contains("withBackupV2RuntimeBuildErrorMapping"))
        XCTAssertFalse(preparation.contains("catch BackupV2RuntimeBuildError"))

        XCTAssertFalse(background.contains("BackupV2RuntimeOpenErrorMapping.withCompatibilityMapping"))
        XCTAssertFalse(background.contains("BackupV2RuntimeOpenErrorMapping.classifyBuildFailure"))
        XCTAssertFalse(background.contains("catch BackupV2RuntimeBuildError"))

        XCTAssertTrue(lease.contains("BackupV2RuntimeOpenErrorMapping.withCompatibilityMapping"))
        XCTAssertTrue(lease.contains("BackupV2RuntimeOpenErrorMapping.classifyBuildFailure"))

        XCTAssertTrue(repoOpen.contains("BackupV2RuntimeOpenErrorMapping.withOpenErrorNormalization"))
        XCTAssertFalse(repoOpen.contains("withShapedRepoBootstrapErrorMapping"))
        XCTAssertFalse(repoOpen.contains("catch let error as RepoBootstrap.BootstrapError"))
        XCTAssertFalse(repoOpen.contains("catch let error as RepoBootstrap.VersionConflict"))
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
            maintenanceStartupMode: .disabled(.test),
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
