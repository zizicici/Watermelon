import CryptoKit
import XCTest
@testable import Watermelon

final class BrowserLinkPairingTests: XCTestCase {
    func testDownloadAdmissionBoundsNodeControlledSizesAndDiskUsage() {
        let photoPath = "/2026/07/IMG_0001.MOV"
        XCTAssertEqual(
            BrowserLinkDownloadAdmissionPolicy.decision(
                size: 200,
                expectedSize: 200,
                availableCapacity: 1024 * 1024 * 1024,
                remotePath: photoPath
            ),
            .accepted
        )
        XCTAssertEqual(
            BrowserLinkDownloadAdmissionPolicy.decision(
                size: 201,
                expectedSize: 200,
                availableCapacity: 1024 * 1024 * 1024,
                remotePath: photoPath
            ),
            .invalidSize
        )
        XCTAssertEqual(
            BrowserLinkDownloadAdmissionPolicy.decision(
                size: 200,
                expectedSize: 200,
                availableCapacity: BrowserLinkDownloadAdmissionPolicy.reservedLocalCapacityBytes + 199,
                remotePath: photoPath
            ),
            .insufficientCapacity
        )
        XCTAssertEqual(
            BrowserLinkDownloadAdmissionPolicy.decision(
                size: 200,
                expectedSize: 200,
                availableCapacity: BrowserLinkDownloadAdmissionPolicy.reservedLocalCapacityBytes + 300,
                reservedCapacity: 101,
                remotePath: photoPath
            ),
            .insufficientCapacity
        )
        XCTAssertEqual(
            BrowserLinkDownloadAdmissionPolicy.decision(
                size: 200,
                expectedSize: 200,
                availableCapacity: nil,
                reservedCapacity: 500,
                remotePath: photoPath
            ),
            .accepted
        )
        XCTAssertEqual(
            BrowserLinkDownloadAdmissionPolicy.maximumBytes(
                forRemotePath: "/.watermelon/months/2026-07.sqlite"
            ),
            BrowserLinkDownloadAdmissionPolicy.maximumRepositoryDatabaseBytes
        )
        XCTAssertEqual(
            BrowserLinkDownloadAdmissionPolicy.maximumBytes(
                forRemotePath: "/.watermelon/locks/00000000-0000-0000-0000-000000000000.lock"
            ),
            BrowserLinkDownloadAdmissionPolicy.maximumLockBytes
        )
    }

    func testBrowserEntryPathsMustMatchTheRequestedNamespace() {
        XCTAssertEqual(
            BrowserLinkStorageClient.canonicalRemotePath("//2026/07/IMG.JPG"),
            "/2026/07/IMG.JPG"
        )
        XCTAssertNil(BrowserLinkStorageClient.canonicalRemotePath("/2026/07/../secret"))
        XCTAssertNil(BrowserLinkStorageClient.joinRemotePath(parent: "/2026/07", name: "../secret"))
        XCTAssertTrue(BrowserLinkStorageClient.validatesRemoteEntryPath(
            path: "/2026/07/IMG.JPG",
            name: "IMG.JPG",
            expectedPath: "/2026/07/IMG.JPG",
            rootNameAllowed: false
        ))
        XCTAssertFalse(BrowserLinkStorageClient.validatesRemoteEntryPath(
            path: "/.watermelon/locks/foreign.lock",
            name: "IMG.JPG",
            expectedPath: "/2026/07/IMG.JPG",
            rootNameAllowed: false
        ))
        XCTAssertFalse(BrowserLinkStorageClient.validatesRemoteEntryPath(
            path: "/2026/07/IMG.JPG",
            name: "OTHER.JPG",
            expectedPath: "/2026/07/IMG.JPG",
            rootNameAllowed: false
        ))
    }
    func testICEPolicyAllowsOnlyLocalHostCandidates() {
        XCTAssertTrue(BrowserLinkICEPolicy.allows(candidateSDP: "candidate:1 1 udp 1 192.168.1.20 5000 typ host"))
        XCTAssertTrue(BrowserLinkICEPolicy.allows(candidateSDP: "candidate:1 1 udp 1 10.0.0.2 5000 typ host"))
        XCTAssertTrue(BrowserLinkICEPolicy.allows(candidateSDP: "candidate:1 1 udp 1 fd12::1 5000 typ host"))
        XCTAssertTrue(BrowserLinkICEPolicy.allows(candidateSDP: "candidate:1 1 udp 1 peer-id.local 5000 typ host"))
        XCTAssertFalse(BrowserLinkICEPolicy.allows(candidateSDP: "candidate:1 1 udp 1 8.8.8.8 5000 typ host"))
        XCTAssertFalse(BrowserLinkICEPolicy.allows(candidateSDP: "candidate:1 1 udp 1 2001:4860::1 5000 typ host"))
        XCTAssertFalse(BrowserLinkICEPolicy.allows(candidateSDP: "candidate:1 1 udp 1 192.168.1.20 5000 typ srflx"))
    }

    func testNetworkPathPolicyAllowsWiFiAndWiredLANButRejectsVPNAndCellular() {
        XCTAssertTrue(BrowserLinkNetworkPathPolicy.allowsLocalTransport(
            isSatisfied: true, usesWiFi: true, usesWiredEthernet: false, usesOther: false
        ))
        XCTAssertTrue(BrowserLinkNetworkPathPolicy.allowsLocalTransport(
            isSatisfied: true, usesWiFi: false, usesWiredEthernet: true, usesOther: false
        ))
        XCTAssertFalse(BrowserLinkNetworkPathPolicy.allowsLocalTransport(
            isSatisfied: true, usesWiFi: true, usesWiredEthernet: false, usesOther: true
        ))
        XCTAssertFalse(BrowserLinkNetworkPathPolicy.allowsLocalTransport(
            isSatisfied: true, usesWiFi: false, usesWiredEthernet: false, usesOther: false
        ))
    }

    func testICEPolicyRemovesPublicCandidatesFromSDP() {
        let sdp = "v=0\ra=candidate:1 1 udp 1 192.168.1.20 5000 typ host\ra=candidate:2 1 udp 1 2001:4860::1 5001 typ host\ra=CANDIDATE:3 1 udp 1 8.8.8.8 5002 typ host\ra=end-of-candidates\r"
        let filtered = BrowserLinkICEPolicy.filteringCandidates(in: sdp)
        XCTAssertTrue(filtered.contains("192.168.1.20"))
        XCTAssertFalse(filtered.contains("2001:4860::1"))
        XCTAssertFalse(filtered.contains("8.8.8.8"))
        XCTAssertTrue(filtered.contains("a=end-of-candidates"))
        XCTAssertEqual(BrowserLinkICEPolicy.statistics(in: sdp).total, 3)
    }

    func testICEPolicyRejectsAmbiguousCandidateSyntaxAndAddresses() {
        for candidate in [
            "not-a-candidate 1 udp 1 192.168.1.20 5000 typ host",
            "candidate: 1 udp 1 192.168.1.20 5000 typ host",
            "candidate:1 1 udp 1 192.168.1.20 5000 typ host\r\na=candidate:2 1 udp 1 8.8.8.8 5001 typ host",
            "candidate:1 1 udp 1 0xC0.0xA8.1.1 5000 typ host",
            "candidate:1 1 udp 1 192.168.1 5000 typ host",
            "candidate:1 1 udp 1 0192.168.1.1 5000 typ host",
            "candidate:1\u{00A0}1 udp 1 192.168.1.20 5000 typ host",
            "candidate:1 1 udp +1 192.168.1.20 +5000 typ host",
            "candidate:1 1 udp 1 192.168.1.20 5000 typ host\u{2028}a=candidate:2 1 udp 1 8.8.8.8 5001 typ host",
            "candidate:1 1 udp 1 .local 5000 typ host",
            "candidate:1 1 udp 1 nested.peer.local 5000 typ host",
            "candidate:1 1 udp 1 -peer.local 5000 typ host",
            "candidate:1 1 udp 1 ::ffff:192.168.1.1 5000 typ host",
            "candidate:1 1 udp 1 127.0.0.1 5000 typ host",
        ] {
            XCTAssertFalse(BrowserLinkICEPolicy.allows(candidateSDP: candidate), candidate)
        }
        XCTAssertTrue(BrowserLinkICEPolicy.allows(candidateSDP: "candidate:abc123 1 udp 1 peer-id.local 5000 typ host"))
        XCTAssertTrue(BrowserLinkICEPolicy.allows(candidateSDP: "candidate:abc 1 udp 1 febf::1 5000 typ host"))
        XCTAssertFalse(BrowserLinkICEPolicy.allows(candidateSDP: "candidate:abc 1 udp 1 fec0::1 5000 typ host"))

        let malformedPrefix = "v=0\r\na = CANDIDATE :1 1 udp 1 192.168.1.20 5000 typ host\r\n"
        XCTAssertFalse(BrowserLinkICEPolicy.filteringCandidates(in: malformedPrefix).contains("192.168.1.20"))
        XCTAssertEqual(BrowserLinkICEPolicy.statistics(in: malformedPrefix).total, 1)
        XCTAssertEqual(BrowserLinkICEPolicy.statistics(in: malformedPrefix).allowed, 0)
        XCTAssertEqual(BrowserLinkICEPolicy.filteringCandidates(in: "v=0\u{2028}a=candidate:1 1 udp 1 192.168.1.20 5000 typ host"), "")
    }

    func testTemporaryNodeUsesRegisteredClientAndMultiplexedWorkers() throws {
        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)
        let profile = BrowserLinkStorageClient.makeProfile(pairing: pairing, folderName: "Backup")
        let nextProfile = BrowserLinkStorageClient.makeProfile(pairing: pairing, folderName: "Backup")
        let client = ProbeStorageClient()
        let factory = StorageClientFactory()
        let token = factory.registerBrowserLink(sessionID: pairing.sessionID, client: client)

        let resolved = try factory.makeClient(profile: profile, password: "")
        XCTAssertTrue((resolved as? ProbeStorageClient) === client)
        XCTAssertEqual(BackupMonthScheduler.resolveWorkerCount(profile: profile, monthCount: 8, override: nil), 2)
        XCTAssertEqual(BackupMonthScheduler.resolveWorkerCount(profile: profile, monthCount: 8, override: 3), 3)
        XCTAssertEqual(BackupMonthScheduler.resolveWorkerCount(profile: profile, monthCount: 8, override: 4), 4)
        XCTAssertEqual(BackupMonthScheduler.resolveWorkerCount(profile: profile, monthCount: 1, override: 4), 1)
        XCTAssertEqual(BackupMonthScheduler.resolveConnectionPoolSize(profile: profile, workerCount: 4, override: 4), 4)
        XCTAssertEqual(BackupRunPreparationService.resolveSyncDownloadConcurrency(profile: profile, override: 4), 1)
        XCTAssertEqual(profile.writerID, nextProfile.writerID)
        XCTAssertEqual(profile.browserLinkSessionID, pairing.sessionID)
        XCTAssertEqual(profile.runtimeConnectionIdentity, profile.credentialRef)

        factory.unregisterBrowserLink(token: token)
        XCTAssertThrowsError(try factory.makeClient(profile: profile, password: ""))
    }

    func testFileSystemRequestTimeoutsTerminateAmbiguousMutations() {
        for operation in [
            "list", "metadata", "upload_begin", "upload_finish", "upload_abort",
            "download_begin", "download_start", "download_finish", "download_abort",
            "create_directory", "delete", "copy", "move", "response_parts",
        ] {
            XCTAssertEqual(
                BrowserLinkClient.fileSystemRequestTimeoutSeconds(operation: operation),
                300,
                operation
            )
        }
        for operation in ["list", "metadata", "response_parts"] {
            XCTAssertFalse(BrowserLinkClient.requestTimeoutClosesSession(operation: operation))
        }
        for operation in [
            "upload_begin", "upload_finish", "upload_abort", "download_begin",
            "download_start", "download_finish", "download_abort", "create_directory",
            "delete", "copy", "move",
        ] {
            XCTAssertTrue(BrowserLinkClient.requestTimeoutClosesSession(operation: operation), operation)
        }
        XCTAssertEqual(BrowserLinkClient.pendingRequestLimit(priority: .ordinary), 6)
        XCTAssertEqual(BrowserLinkClient.pendingRequestLimit(priority: .control), 7)
        XCTAssertEqual(BrowserLinkClient.pendingRequestLimit(priority: .cleanup), 8)
        XCTAssertEqual(BrowserLinkClient.requestSlotWaiterLimit(priority: .ordinary), 12)
        XCTAssertEqual(BrowserLinkClient.requestSlotWaiterLimit(priority: .control), 15)
        XCTAssertEqual(BrowserLinkClient.requestSlotWaiterLimit(priority: .cleanup), 16)

        let firstOrdinary = UUID()
        let secondOrdinary = UUID()
        let control = UUID()
        let cleanup = UUID()
        let order = [firstOrdinary, secondOrdinary, control, cleanup]
        let priorities: [UUID: BrowserLinkClient.FileSystemRequestPriority] = [
            firstOrdinary: .ordinary,
            secondOrdinary: .ordinary,
            control: .control,
            cleanup: .cleanup,
        ]
        XCTAssertEqual(BrowserLinkClient.nextRequestSlotWaiterID(
            order: order,
            priorities: priorities,
            pendingRequestCount: 6
        ), cleanup)
        XCTAssertEqual(BrowserLinkClient.nextRequestSlotWaiterID(
            order: Array(order.dropLast()),
            priorities: priorities,
            pendingRequestCount: 6
        ), control)
        XCTAssertEqual(BrowserLinkClient.nextRequestSlotWaiterID(
            order: [firstOrdinary, secondOrdinary],
            priorities: priorities,
            pendingRequestCount: 0
        ), firstOrdinary)
    }

    func testTerminatedDownloadConsumerAbandonsOnlyItsTransfer() {
        XCTAssertEqual(
            BrowserLinkClient.downloadYieldDisposition(.enqueued(remaining: 1)),
            .accepted
        )
        XCTAssertEqual(
            BrowserLinkClient.downloadYieldDisposition(.terminated),
            .abandoned
        )
        XCTAssertEqual(
            BrowserLinkClient.downloadYieldDisposition(.dropped(Data())),
            .protocolFailure
        )
    }

    func testTemporaryNodeCanonicalizesPersistedWriterID() throws {
        let uppercase = "E0A9AE02-2965-4D8A-9924-A22E067B48F1"
        let canonical = try XCTUnwrap(BrowserLinkStorageClient.canonicalWriterID(uppercase))

        XCTAssertEqual(canonical, uppercase.lowercased())
        XCTAssertNotNil(RepoLayoutLite.lockFilename(writerID: canonical))
        XCTAssertNil(BrowserLinkStorageClient.canonicalWriterID("not-a-writer-id"))
    }

    func testTimestampGuideAndPlainTextScriptsUseSystemSQLite() throws {
        let guide = try XCTUnwrap(String(
            data: BrowserLinkTimestampArtifacts.guideData(languageIdentifier: "en"),
            encoding: .utf8
        ))
        XCTAssertTrue(guide.contains("Nothing runs automatically"))
        XCTAssertTrue(guide.contains(BrowserLinkTimestampArtifacts.windowsScriptName))
        XCTAssertTrue(guide.contains(BrowserLinkTimestampArtifacts.macScriptName))

        let windowsScript = try XCTUnwrap(String(
            data: BrowserLinkTimestampArtifacts.windowsScriptData(languageIdentifier: "en"),
            encoding: .utf8
        ))
        XCTAssertTrue(windowsScript.contains("winsqlite3.dll"))
        XCTAssertTrue(windowsScript.contains("LOAD_LIBRARY_SEARCH_SYSTEM32"))
        XCTAssertTrue(windowsScript.contains(".watermelon\\months"))
        XCTAssertTrue(windowsScript.contains("Write-Progress"))
        XCTAssertTrue(windowsScript.contains("Completed. Updated: $updated"))
        XCTAssertFalse(windowsScript.contains(".tsv"))

        let macScript = try XCTUnwrap(String(
            data: BrowserLinkTimestampArtifacts.macScriptData(languageIdentifier: "en"),
            encoding: .utf8
        ))
        XCTAssertTrue(macScript.contains("/usr/bin/sqlite3"))
        XCTAssertTrue(macScript.contains(".watermelon/months"))
        XCTAssertTrue(macScript.contains("milliseconds - 999"))
        XCTAssertTrue(macScript.contains("/usr/bin/perl"))
        XCTAssertFalse(macScript.contains("/bin/date -r"))
        XCTAssertFalse(macScript.contains("/usr/bin/touch -mt"))
        XCTAssertTrue(macScript.contains("decodedName"))
        XCTAssertTrue(macScript.contains("%3d%%"))
        XCTAssertTrue(macScript.contains("'Completed'"))
        XCTAssertFalse(macScript.contains(".tsv"))
    }

    func testTimestampGuideAndFolderFollowAppLanguage() throws {
        let simplified = try XCTUnwrap(String(
            data: BrowserLinkTimestampArtifacts.guideData(languageIdentifier: "zh-Hans"),
            encoding: .utf8
        ))
        XCTAssertTrue(simplified.contains("<html lang=\"zh-Hans\">"))
        XCTAssertTrue(simplified.contains("恢复文件的原始日期"))
        XCTAssertTrue(simplified.contains("./如何恢复文件修改日期/"))
        XCTAssertTrue(simplified.contains("看到完成汇总后再关闭终端窗口"))
        let simplifiedMacScript = try XCTUnwrap(String(
            data: BrowserLinkTimestampArtifacts.macScriptData(languageIdentifier: "zh-Hans"),
            encoding: .utf8
        ))
        XCTAssertTrue(simplifiedMacScript.contains("'正在恢复文件修改日期'"))
        XCTAssertTrue(simplifiedMacScript.contains("'已完成'"))
        XCTAssertEqual(
            BrowserLinkTimestampArtifacts.folderName(languageIdentifier: "zh-Hans-CN"),
            "如何恢复文件修改日期"
        )
        XCTAssertEqual(
            BrowserLinkTimestampArtifacts.folderName(languageIdentifier: "zh-HK"),
            "如何還原檔案修改日期"
        )
        XCTAssertEqual(
            BrowserLinkTimestampArtifacts.folderName(languageIdentifier: "ja-JP"),
            "ファイルの更新日時を復元する方法"
        )
        XCTAssertEqual(
            BrowserLinkTimestampArtifacts.folderName(languageIdentifier: "unknown"),
            "How to restore file modification dates"
        )
    }

    func testTimestampArtifactsUploadToTheSelectedBackupRoot() async throws {
        let client = InMemoryRemoteStorageClient()
        let existingGuide = Data("user content".utf8)
        let folder = BrowserLinkTimestampArtifacts.folderName
        await client.seedFile(
            path: "/Backup/\(folder)/\(BrowserLinkTimestampArtifacts.guideName)",
            data: existingGuide
        )
        let installed = await BrowserLinkTimestampArtifacts.installTools(client: client, basePath: "/Backup")
        XCTAssertTrue(installed)

        let guide = await client.fileData(path: "/Backup/\(folder)/\(BrowserLinkTimestampArtifacts.guideName)")
        let windowsScript = await client.fileData(path: "/Backup/\(folder)/\(BrowserLinkTimestampArtifacts.windowsScriptName)")
        let macScript = await client.fileData(path: "/Backup/\(folder)/\(BrowserLinkTimestampArtifacts.macScriptName)")

        XCTAssertEqual(guide, existingGuide)
        XCTAssertNotNil(windowsScript)
        XCTAssertNotNil(macScript)
        let createdDirectories = await client.createdDirectories
        XCTAssertTrue(createdDirectories.contains("/Backup/\(folder)"))
        let rootGuide = await client.fileData(path: "/Backup/\(BrowserLinkTimestampArtifacts.guideName)")
        XCTAssertNil(rootGuide)
    }

    func testTimestampArtifactInstallReportsRetryableFailure() async {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueUploadError(RemoteStorageClientError.unavailable)

        let installed = await BrowserLinkTimestampArtifacts.installTools(client: client, basePath: "/Backup")
        XCTAssertFalse(installed)
    }

    func testTemporaryNodeCarriesCanonicalBrowserTakeoverScope() throws {
        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)
        let nodeID = Data(64..<96).base64URLEncodedString()
        let reclaimNodeID = Data(96..<128).base64URLEncodedString()
        let profile = BrowserLinkStorageClient.makeProfile(
            pairing: pairing,
            folderName: "Backup",
            browserNodeID: nodeID,
            reclaimBrowserNodeIDs: [reclaimNodeID]
        )

        XCTAssertEqual(profile.browserLinkCurrentLockScope, nodeID)
        XCTAssertEqual(profile.browserLinkFreshTakeoverScopes, [reclaimNodeID])
        XCTAssertNil(BrowserLinkStorageClient.canonicalBrowserNodeID("invalid"))
    }

    func testTemporaryNodeRejectsMalformedBrowserTakeoverScopes() throws {
        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)
        let current = Data(64..<96).base64URLEncodedString()
        let reclaim = Data(96..<128).base64URLEncodedString()
        var profile = BrowserLinkStorageClient.makeProfile(pairing: pairing, folderName: "Backup")

        for payload in [
            ["current": "invalid", "reclaim": [reclaim]],
            ["current": current, "reclaim": [reclaim, reclaim]],
            ["current": current, "reclaim": Array(repeating: reclaim, count: 17)],
            ["current": current, "reclaim": ["invalid"]],
        ] as [[String: Any]] {
            profile.connectionParams = try JSONSerialization.data(withJSONObject: payload)
            XCTAssertNil(profile.browserLinkCurrentLockScope)
            XCTAssertTrue(profile.browserLinkFreshTakeoverScopes.isEmpty)
        }
    }

    func testCandidateDiagnosticLabelsAreBoundedAndAccurate() {
        XCTAssertEqual(
            BrowserLinkICEPolicy.diagnosticLabel(
                for: "candidate:1 1 udp 1 192.168.1.20 5000 typ host"
            ),
            "host/private-ipv4/allowed"
        )
        XCTAssertEqual(
            BrowserLinkICEPolicy.diagnosticLabel(
                for: "candidate:1 1 udp 1 8.8.8.8 5000 typ host"
            ),
            "host/public-ipv4/rejected"
        )
        XCTAssertEqual(BrowserLinkICEPolicy.diagnosticLabel(for: "candidate:bad"), "malformed")
    }

    func testStaleRegistrationCannotRemoveReplacement() throws {
        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)
        let profile = BrowserLinkStorageClient.makeProfile(pairing: pairing, folderName: "Backup")
        let first = ProbeStorageClient()
        let replacement = ProbeStorageClient()
        let factory = StorageClientFactory()
        let staleToken = factory.registerBrowserLink(sessionID: pairing.sessionID, client: first)
        let currentToken = factory.registerBrowserLink(sessionID: pairing.sessionID, client: replacement)

        factory.unregisterBrowserLink(token: staleToken)
        XCTAssertTrue((try factory.makeClient(profile: profile, password: "") as? ProbeStorageClient) === replacement)
        factory.unregisterBrowserLink(token: currentToken)
        XCTAssertThrowsError(try factory.makeClient(profile: profile, password: ""))
    }

    func testEphemeralConnectLeaseBlocksExecution() {
        let flags = AppRuntimeFlags()
        XCTAssertTrue(flags.tryBeginEphemeralConnecting(sessionID: "browser-link:test"))
        XCTAssertFalse(flags.tryBeginEphemeralConnecting(sessionID: "browser-link:replacement"))
        XCTAssertFalse(flags.tryBeginConnecting(profileID: 7))
        XCTAssertFalse(flags.tryEnterExecution())
        XCTAssertNil(flags.withProfileMutationLease(profileID: 7) { true })
        flags.endEphemeralConnecting(sessionID: "browser-link:test")
        XCTAssertTrue(flags.tryEnterExecution())
        flags.exitExecution()
    }

    func testParsesCanonicalPairingURL() throws {
        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)

        XCTAssertEqual(pairing.ticket, fixture.ticket)
        XCTAssertEqual(pairing.secret, fixture.secret)
        XCTAssertEqual(pairing.sessionID, fixture.session.base64URLEncodedString())
        XCTAssertEqual(pairing.signalingURL.scheme, "wss")
        XCTAssertEqual(pairing.signalingURL.host, BrowserLinkPairing.host)
        XCTAssertEqual(pairing.signalingURL.path, "/ws/v1")
    }

    func testParsesLocalizedPairingPath() throws {
        let fixture = makeFixture(path: "/zh-Hans/pair")
        XCTAssertNoThrow(try BrowserLinkPairing.parse(fixture.url, now: fixture.now))
        let trailingSlash = makeFixture(path: "/zh-Hans/pair/")
        XCTAssertNoThrow(try BrowserLinkPairing.parse(trailingSlash.url, now: trailingSlash.now))
    }

    func testCandidateURLFiltersUnrelatedLaunchActivities() {
        XCTAssertTrue(BrowserLinkPairing.isCandidateURL(URL(string: "https://link.watermelonbackup.com/zh-Hans/pair")!))
        XCTAssertFalse(BrowserLinkPairing.isCandidateURL(URL(string: "https://link.watermelonbackup.com/support")!))
        XCTAssertFalse(BrowserLinkPairing.isCandidateURL(URL(string: "https://link.watermelonbackup.com/en/pair")!))
        XCTAssertFalse(BrowserLinkPairing.isCandidateURL(URL(string: "https://link.watermelonbackup.com/zh-HK/pair")!))
        XCTAssertFalse(BrowserLinkPairing.isCandidateURL(URL(string: "https://link.watermelonbackup.com/it/pair")!))
        XCTAssertFalse(BrowserLinkPairing.isCandidateURL(URL(string: "https://example.com/pair")!))
    }

    func testRejectsWrongHostAndExtraFragmentFields() {
        let fixture = makeFixture()
        var components = URLComponents(url: fixture.url, resolvingAgainstBaseURL: false)!
        components.host = "example.com"
        XCTAssertThrowsError(try BrowserLinkPairing.parse(components.url!, now: fixture.now))

        components = URLComponents(url: fixture.url, resolvingAgainstBaseURL: false)!
        components.fragment = components.fragment! + "&x=1"
        XCTAssertThrowsError(try BrowserLinkPairing.parse(components.url!, now: fixture.now))

        components = URLComponents(url: fixture.url, resolvingAgainstBaseURL: false)!
        components.port = 8443
        XCTAssertThrowsError(try BrowserLinkPairing.parse(components.url!, now: fixture.now))
    }

    func testRejectsPairingURLAuthorityQueryAndFragmentAmbiguity() {
        let fixture = makeFixture()
        var components = URLComponents(url: fixture.url, resolvingAgainstBaseURL: false)!
        components.scheme = "http"
        XCTAssertThrowsError(try BrowserLinkPairing.parse(components.url!, now: fixture.now))

        components = URLComponents(url: fixture.url, resolvingAgainstBaseURL: false)!
        components.user = "user"
        XCTAssertThrowsError(try BrowserLinkPairing.parse(components.url!, now: fixture.now))

        components = URLComponents(url: fixture.url, resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "source", value: "camera")]
        XCTAssertThrowsError(try BrowserLinkPairing.parse(components.url!, now: fixture.now))

        components = URLComponents(url: fixture.url, resolvingAgainstBaseURL: false)!
        components.fragment = "t=\(fixture.ticket)&t=\(fixture.ticket)&s=\(fixture.secret.base64URLEncodedString())"
        XCTAssertThrowsError(try BrowserLinkPairing.parse(components.url!, now: fixture.now))

        components = URLComponents(url: fixture.url, resolvingAgainstBaseURL: false)!
        components.fragment = "t=\(fixture.ticket)&s=\(fixture.secret.base64URLEncodedString())="
        XCTAssertThrowsError(try BrowserLinkPairing.parse(components.url!, now: fixture.now))
    }

    func testRejectsUnsupportedVersionFutureIssueAndExcessiveTicketTTL() throws {
        let fixture = makeFixture()
        var bytes = try XCTUnwrap(Data(base64URLEncoded: fixture.ticket))
        bytes[0] = 2
        XCTAssertThrowsError(try BrowserLinkPairing.parse(
            pairingURL(ticketBytes: bytes, secret: fixture.secret),
            now: fixture.now
        ))

        bytes = try XCTUnwrap(Data(base64URLEncoded: fixture.ticket))
        bytes.replaceSubrange(49..<53, with: uint32Bytes(1_800_000_031))
        bytes.replaceSubrange(53..<57, with: uint32Bytes(1_800_000_179))
        XCTAssertThrowsError(try BrowserLinkPairing.parse(
            pairingURL(ticketBytes: bytes, secret: fixture.secret),
            now: fixture.now
        ))

        bytes = try XCTUnwrap(Data(base64URLEncoded: fixture.ticket))
        bytes.replaceSubrange(49..<53, with: uint32Bytes(1_799_999_000))
        bytes.replaceSubrange(53..<57, with: uint32Bytes(1_800_000_179))
        XCTAssertThrowsError(try BrowserLinkPairing.parse(
            pairingURL(ticketBytes: bytes, secret: fixture.secret),
            now: fixture.now
        ))
    }

    func testRejectsSecretThatDoesNotMatchTicketCommitment() {
        let fixture = makeFixture()
        var components = URLComponents(url: fixture.url, resolvingAgainstBaseURL: false)!
        components.fragment = "t=\(fixture.ticket)&s=\(Data(repeating: 0xff, count: 32).base64URLEncodedString())"
        XCTAssertThrowsError(try BrowserLinkPairing.parse(components.url!, now: fixture.now))
    }

    func testRejectsExpiredTicket() {
        let fixture = makeFixture()
        XCTAssertThrowsError(
            try BrowserLinkPairing.parse(
                fixture.url,
                now: fixture.now.addingTimeInterval(181)
            )
        ) { error in
            XCTAssertEqual(error as? BrowserLinkPairingError, .expired)
        }
    }

    func testSignalCipherRoundTripsAndRejectsTampering() throws {
        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)
        let browserCipher = BrowserLinkSignalCipher(pairing: pairing, role: .browser)
        let phoneCipher = BrowserLinkSignalCipher(pairing: pairing)
        let encrypted = try browserCipher.encrypt([
            "type": "answer",
            "description": ["type": "answer", "sdp": "v=0\r\n"],
        ])
        let decrypted = try phoneCipher.decrypt(encrypted)
        XCTAssertEqual(decrypted["type"] as? String, "answer")

        var parts = encrypted.split(separator: ".").map(String.init)
        parts[1].replaceSubrange(parts[1].startIndex...parts[1].startIndex, with: parts[1].first == "A" ? "B" : "A")
        XCTAssertThrowsError(try phoneCipher.decrypt(parts.joined(separator: ".")))
    }

    func testSignalCipherMatchesBrowserAESGCMVector() throws {
        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)
        let browserCipher = BrowserLinkSignalCipher(pairing: pairing, role: .browser)
        let phoneCipher = BrowserLinkSignalCipher(pairing: pairing)
        let encrypted = try browserCipher.encrypt(
            ["type": "offer"],
            nonceData: Data(0..<12)
        )
        XCTAssertEqual(
            encrypted,
            "AAECAwQFBgcICQoL.8y3h-aBy4OOXSdgN2xeSOPf6EDkyLutWaWQ8rDo3wSk"
        )
        XCTAssertEqual(try phoneCipher.decrypt(encrypted)["type"] as? String, "offer")
    }

    func testAuthenticationMACMatchesBrowserVector() {
        let mac = BrowserLinkSignalCipher.authenticationMAC(
            secret: Data(0..<32),
            sessionID: "ICEiIyQlJicoKSorLC0uLw",
            nonce: "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3"
        )
        XCTAssertEqual(mac, "UHFtYOO3ymrcr0ChHYNh_NhsfRp6t1SJfD7fsyb6udM")

        let confirmation = BrowserLinkSignalCipher.authenticationConfirmationMAC(
            secret: Data(0..<32),
            sessionID: "ICEiIyQlJicoKSorLC0uLw",
            nonce: "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3",
            folderName: "Backup",
            browserNodeID: "QEFCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaW1xdXl8",
            reclaimBrowserNodeIDs: [],
            uploadChunkBytes: 131_072
        )
        XCTAssertEqual(confirmation, "-O9Ptycj4bT1JrxBgci510Fb0ST5B2R1e2VkQsv3o7w")
    }

    func testAuthenticationRequiresChallengeBeforeConfirmation() {
        var gate = BrowserLinkAuthenticationGate()
        XCTAssertFalse(gate.acceptConfirmation())
        XCTAssertTrue(gate.acceptChallenge())
        XCTAssertFalse(gate.acceptChallenge())
        XCTAssertTrue(gate.acceptConfirmation())
        XCTAssertFalse(gate.acceptConfirmation())
    }

    func testBinaryUploadFrameMatchesBrowserVector() throws {
        let frame = try BrowserLinkFileFrameCodec.encode(
            kind: .upload,
            transferID: "00112233-4455-6677-8899-aabbccddeeff",
            offset: 0x0102_0304,
            payload: Data([0xde, 0xad, 0xbe, 0xef])
        )
        XCTAssertEqual(
            frame.base64EncodedString(),
            "V01MAQARIjNEVWZ3iJmqu8zd7v8AAAAAAQIDBAAAAATerb7v"
        )
        XCTAssertThrowsError(try BrowserLinkFileFrameCodec.encode(
            kind: .upload,
            transferID: "00112233-4455-6677-8899-aabbccddeeff",
            offset: 0,
            payload: Data()
        ))
    }

    func testBinaryDownloadFrameMatchesBrowserVector() throws {
        let data = try XCTUnwrap(Data(
            base64Encoded: "V01MAgARIjNEVWZ3iJmqu8zd7v8AAAAAAQIDBAAAAATerb7v"
        ))
        let frame = try BrowserLinkFileFrameCodec.decode(data, expectedKind: .download)

        XCTAssertEqual(frame.transferID, "00112233-4455-6677-8899-aabbccddeeff")
        XCTAssertEqual(frame.offset, 0x0102_0304)
        XCTAssertEqual(frame.payload, Data([0xde, 0xad, 0xbe, 0xef]))
        XCTAssertThrowsError(try BrowserLinkFileFrameCodec.decode(data, expectedKind: .upload))
    }

    func testDownloadReceivePolicyRequiresContiguousBoundedFrames() throws {
        XCTAssertEqual(try BrowserLinkDownloadReceivePolicy.nextReceivedSize(
            expectedSize: 8 * 1024 * 1024,
            receivedSize: 128 * 1024,
            acknowledgedSize: 0,
            totalUnacknowledgedBytes: 128 * 1024,
            frameOffset: 128 * 1024,
            payloadSize: 128 * 1024
        ), 256 * 1024)
        XCTAssertThrowsError(try BrowserLinkDownloadReceivePolicy.nextReceivedSize(
            expectedSize: 8 * 1024 * 1024,
            receivedSize: 128 * 1024,
            acknowledgedSize: 0,
            totalUnacknowledgedBytes: 128 * 1024,
            frameOffset: 64 * 1024,
            payloadSize: 128 * 1024
        ))
        XCTAssertThrowsError(try BrowserLinkDownloadReceivePolicy.nextReceivedSize(
            expectedSize: 8 * 1024 * 1024,
            receivedSize: 4 * 1024 * 1024,
            acknowledgedSize: 0,
            totalUnacknowledgedBytes: 4 * 1024 * 1024,
            frameOffset: 4 * 1024 * 1024,
            payloadSize: 1
        ))
        XCTAssertThrowsError(try BrowserLinkDownloadReceivePolicy.nextReceivedSize(
            expectedSize: 100,
            receivedSize: 90,
            acknowledgedSize: 90,
            totalUnacknowledgedBytes: 0,
            frameOffset: 90,
            payloadSize: 11
        ))
        XCTAssertThrowsError(try BrowserLinkDownloadReceivePolicy.nextReceivedSize(
            expectedSize: 32 * 1024,
            receivedSize: 0,
            acknowledgedSize: 0,
            totalUnacknowledgedBytes: 0,
            frameOffset: 0,
            payloadSize: 1
        ))
        XCTAssertThrowsError(try BrowserLinkDownloadReceivePolicy.nextReceivedSize(
            expectedSize: 8 * 1024 * 1024,
            receivedSize: 0,
            acknowledgedSize: 0,
            totalUnacknowledgedBytes: 4 * 1024 * 1024,
            frameOffset: 0,
            payloadSize: 8 * 1024
        ))
    }

    func testDataChannelIngressRestoresDelegateOrderBeforeProcessing() throws {
        var buffer = BrowserLinkOrderedIngressBuffer<String>()

        XCTAssertEqual(try buffer.insert(sequence: 1, value: "second", maximumPending: 4), [])
        XCTAssertEqual(
            try buffer.insert(sequence: 0, value: "first", maximumPending: 4),
            ["first", "second"]
        )
        XCTAssertEqual(try buffer.insert(sequence: 3, value: "fourth", maximumPending: 4), [])
        XCTAssertEqual(try buffer.insert(sequence: 2, value: "third", maximumPending: 4), ["third", "fourth"])
        XCTAssertThrowsError(try buffer.insert(sequence: 2, value: "duplicate", maximumPending: 4))
    }

    func testDataChannelIngressReservationIsBoundedBeforeMainActorDelivery() {
        let sequencer = BrowserLinkIngressSequencer()
        XCTAssertEqual(sequencer.reserveSequence(messageBytes: 4, maximumMessages: 2, maximumBytes: 8), 0)
        XCTAssertEqual(sequencer.reserveSequence(messageBytes: 4, maximumMessages: 2, maximumBytes: 8), 1)
        XCTAssertNil(sequencer.reserveSequence(messageBytes: 1, maximumMessages: 2, maximumBytes: 8))
        XCTAssertTrue(sequencer.claimOverflowNotification())
        XCTAssertFalse(sequencer.claimOverflowNotification())
        XCTAssertNil(sequencer.reserveSequence(messageBytes: 1, maximumMessages: 2, maximumBytes: 8))
        XCTAssertFalse(sequencer.claimOverflowNotification())
        sequencer.reset()
        XCTAssertEqual(sequencer.reserveSequence(messageBytes: 8, maximumMessages: 2, maximumBytes: 8), 2)
        sequencer.release(messageBytes: 8)
    }

    func testCumulativeUploadAcknowledgementIgnoresStaleMainActorDelivery() throws {
        XCTAssertNil(try BrowserLinkClient.acceptedAcknowledgement(
            current: 2 * 1024 * 1024,
            sent: 3 * 1024 * 1024,
            received: 1024 * 1024
        ))
        XCTAssertEqual(try BrowserLinkClient.acceptedAcknowledgement(
            current: 2 * 1024 * 1024,
            sent: 3 * 1024 * 1024,
            received: 3 * 1024 * 1024
        ), 3 * 1024 * 1024)
        XCTAssertThrowsError(try BrowserLinkClient.acceptedAcknowledgement(
            current: 2 * 1024 * 1024,
            sent: 3 * 1024 * 1024,
            received: 4 * 1024 * 1024
        ))
    }

    func testLinkStartPolicyBlocksConnectingAndConnectedNodes() {
        XCTAssertNil(BrowserLinkStartPolicy.blockReason(
            isConnected: false,
            isConnecting: false,
            canInteractWithRemoteNode: true
        ))
        XCTAssertEqual(BrowserLinkStartPolicy.blockReason(
            isConnected: false,
            isConnecting: true,
            canInteractWithRemoteNode: true
        ), .busy)
        XCTAssertEqual(BrowserLinkStartPolicy.blockReason(
            isConnected: true,
            isConnecting: false,
            canInteractWithRemoteNode: true
        ), .existingConnection)
    }

    func testSignalingCloseReasonMapsDesktopDeparture() {
        let fallback = BrowserLinkClientError.connectionClosed
        if case .peerLeft? = BrowserLinkClient.signalingCloseError(
            closeReason: Data("peer_left".utf8),
            underlying: fallback
        ) as? BrowserLinkClientError {} else {
            XCTFail("Expected peerLeft")
        }
        if case .connectionClosed? = BrowserLinkClient.signalingCloseError(
            closeReason: nil,
            underlying: fallback
        ) as? BrowserLinkClientError {} else {
            XCTFail("Expected fallback error")
        }
    }

    func testFileSystemResponseAssemblerReassemblesOrderedPayload() throws {
        var assembler = BrowserLinkFileSystemResponseAssembler()
        XCTAssertNil(try assembler.append(index: 1, total: 2, part: Data("world".utf8)))
        XCTAssertEqual(
            try assembler.append(index: 0, total: 2, part: Data("hello ".utf8)),
            Data("hello world".utf8)
        )
    }

    func testFileSystemResponseAssemblerRejectsInvalidParts() throws {
        var assembler = BrowserLinkFileSystemResponseAssembler()
        XCTAssertNil(try assembler.append(index: 0, total: 2, part: Data("one".utf8)))
        XCTAssertThrowsError(try assembler.append(index: 0, total: 2, part: Data("duplicate".utf8)))

        var inconsistent = BrowserLinkFileSystemResponseAssembler()
        XCTAssertNil(try inconsistent.append(index: 0, total: 2, part: Data()))
        XCTAssertThrowsError(try inconsistent.append(index: 1, total: 3, part: Data()))
    }

    func testDestinationErrorsDoNotMasqueradeAsTemporaryConnectionFailures() throws {
        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)
        let profile = BrowserLinkStorageClient.makeProfile(pairing: pairing, folderName: "Backup")
        XCTAssertFalse(profile.isConnectionUnavailableError(
            RemoteStorageClientError.underlying(CocoaError(.fileWriteOutOfSpace))
        ))
        XCTAssertFalse(profile.isConnectionUnavailableError(
            RemoteStorageClientError.underlying(CocoaError(.fileWriteNoPermission))
        ))
        XCTAssertFalse(profile.isExternalStorageUnavailableError(
            RemoteStorageClientError.externalStorageUnavailable
        ))
        XCTAssertEqual(
            profile.userFacingStorageErrorMessage(RemoteStorageClientError.externalStorageUnavailable),
            RemoteStorageClientError.externalStorageUnavailable.localizedDescription
        )
    }

    func testFileSystemRequestPriorityMirrorsBrowserLockRouting() {
        let lock = "/.watermelon/locks/00112233-4455-6677-8899-aabbccddeeff.lock"
        for operation in ["list", "metadata", "download_begin"] {
            XCTAssertEqual(BrowserLinkStorageClient.requestPriority(
                operation: operation,
                arguments: ["path": lock]
            ), .control, operation)
            XCTAssertEqual(BrowserLinkStorageClient.requestPriority(
                operation: operation,
                arguments: ["path": "/2026/07/photo.heic"]
            ), .ordinary, operation)
        }
        for operation in ["create_directory", "delete", "upload_begin"] {
            for path in ["/", "/.watermelon", "/.watermelon/locks", lock] {
                XCTAssertEqual(BrowserLinkStorageClient.requestPriority(
                    operation: operation,
                    arguments: ["path": path]
                ), .control, "\(operation) \(path)")
            }
            XCTAssertEqual(BrowserLinkStorageClient.requestPriority(
                operation: operation,
                arguments: ["path": "/.watermelon/months"]
            ), .ordinary, operation)
        }
        for operation in ["copy", "move"] {
            XCTAssertEqual(BrowserLinkStorageClient.requestPriority(
                operation: operation,
                arguments: ["sourcePath": lock, "destinationPath": "/copy"]
            ), .control, operation)
            XCTAssertEqual(BrowserLinkStorageClient.requestPriority(
                operation: operation,
                arguments: ["sourcePath": "/source", "destinationPath": "/.watermelon/locks/copy"]
            ), .control, operation)
            XCTAssertEqual(BrowserLinkStorageClient.requestPriority(
                operation: operation,
                arguments: ["sourcePath": "/source", "destinationPath": "/copy"]
            ), .ordinary, operation)
        }
        XCTAssertEqual(BrowserLinkStorageClient.requestPriority(
            operation: "upload_abort",
            arguments: [:]
        ), .cleanup)
        XCTAssertEqual(BrowserLinkStorageClient.requestPriority(
            operation: "download_abort",
            arguments: [:]
        ), .cleanup)
    }

    func testDownloadTransportAndCapacityErrorsMapToRetryableStorageFaults() throws {
        let channelClosed = BrowserLinkStorageClient.mappedFileSystemError(
            BrowserLinkFileSystemError.remote("channel_closed"),
            arguments: ["path": "/photo.heic"]
        )
        let saturated = BrowserLinkStorageClient.mappedFileSystemError(
            BrowserLinkFileSystemError.remote("too_many_transfers"),
            arguments: ["path": "/photo.heic"]
        )
        let timedOut = BrowserLinkStorageClient.mappedFileSystemError(
            BrowserLinkFileSystemError.remote("transfer_timeout"),
            arguments: ["path": "/photo.heic"]
        )

        XCTAssertEqual(RemoteFaultLite.classify(channelClosed), .retryable)
        XCTAssertEqual(RemoteFaultLite.classify(saturated), .retryable)
        XCTAssertEqual(RemoteFaultLite.classify(timedOut), .retryable)

        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)
        let profile = BrowserLinkStorageClient.makeProfile(pairing: pairing, folderName: "Backup")
        XCTAssertFalse(profile.isConnectionUnavailableError(saturated))
        XCTAssertFalse(profile.isConnectionUnavailableError(timedOut))
        XCTAssertTrue(BrowserLinkStorageClient.isRetryableTransferError(saturated))
        XCTAssertTrue(BrowserLinkStorageClient.isRetryableTransferError(timedOut))
    }

    func testUploadStreamFailureWinsOverTheTerminalRequestError() throws {
        let chosen = BrowserLinkStorageClient.preferredUploadError(
            requestError: BrowserLinkFileSystemError.remote("unknown_transfer"),
            streamFailure: BrowserLinkFileSystemError.remote("transfer_timeout")
        )
        let fileSystemError = try XCTUnwrap(chosen as? BrowserLinkFileSystemError)
        guard case .remote(let code) = fileSystemError else {
            return XCTFail("Expected a remote filesystem error")
        }
        XCTAssertEqual(code, "transfer_timeout")
        XCTAssertEqual(BrowserLinkClient.uploadFlowTimeout, .seconds(65))
        XCTAssertEqual(BrowserLinkClient.abandonedDownloadRetention, .seconds(305))
        let dataLimit: Int64 = 3 * 1024 * 1024 + 512 * 1024
        XCTAssertEqual(BrowserLinkClient.availableUploadWindowBytes(
            totalOutstandingBytes: dataLimit,
            classOutstandingBytes: dataLimit,
            control: false
        ), 0)
        XCTAssertEqual(BrowserLinkClient.availableUploadWindowBytes(
            totalOutstandingBytes: dataLimit,
            classOutstandingBytes: 0,
            control: true
        ), 512 * 1024)
        XCTAssertEqual(
            BrowserLinkClient.bufferedUploadLimit(control: false),
            BrowserLinkFileFrameCodec.headerSize + BrowserLinkFileFrameCodec.maximumPayloadBytes
        )
        XCTAssertEqual(BrowserLinkClient.bufferedUploadLimit(control: true), 192 * 1024)
    }

    func testQueuedLockDownloadGateRespondsToCancellation() async throws {
        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)
        let client = await MainActor.run { BrowserLinkClient(pairing: pairing) }
        let storage = BrowserLinkStorageClient(client: client)
        try await storage.acquireLockDownloadSlot()

        let waiting = Task { try await storage.acquireLockDownloadSlot() }
        try await Task.sleep(for: .milliseconds(10))
        waiting.cancel()
        do {
            try await waiting.value
            XCTFail("Cancelled lock waiter unexpectedly acquired the slot")
        } catch {
            XCTAssertTrue(error is CancellationError)
        }

        await storage.releaseLockDownloadSlot()
        try await storage.acquireLockDownloadSlot()
        await storage.releaseLockDownloadSlot()
    }

    func testDataDownloadGateAllowsTwoAndQueuesTheThird() async throws {
        let fixture = makeFixture()
        let pairing = try BrowserLinkPairing.parse(fixture.url, now: fixture.now)
        let client = await MainActor.run { BrowserLinkClient(pairing: pairing) }
        let storage = BrowserLinkStorageClient(client: client)
        try await storage.acquireDataDownloadSlot()
        try await storage.acquireDataDownloadSlot()

        let waiting = Task { try await storage.acquireDataDownloadSlot() }
        try await Task.sleep(for: .milliseconds(10))
        XCTAssertFalse(waiting.isCancelled)

        await storage.releaseDataDownloadSlot()
        try await waiting.value
        await storage.releaseDataDownloadSlot()
        await storage.releaseDataDownloadSlot()
    }

    private func makeFixture(path: String = "/pair") -> (
        url: URL,
        ticket: String,
        secret: Data,
        session: Data,
        now: Date
    ) {
        let now = Date(timeIntervalSince1970: 1_800_000_000)
        let secret = Data(0..<32)
        let session = Data(32..<48)
        var ticketBytes = Data([1])
        ticketBytes.append(session)
        var commitmentInput = Data("watermelon-link-capability-v1:".utf8)
        commitmentInput.append(secret)
        ticketBytes.append(Data(SHA256.hash(data: commitmentInput)))
        ticketBytes.appendUInt32BigEndian(1_799_999_999)
        ticketBytes.appendUInt32BigEndian(1_800_000_089)
        ticketBytes.append(Data(repeating: 0, count: 16))
        let ticket = ticketBytes.base64URLEncodedString()
        let url = URL(string: "https://\(BrowserLinkPairing.host)\(path)#t=\(ticket)&s=\(secret.base64URLEncodedString())")!
        return (url, ticket, secret, session, now)
    }

    private func pairingURL(ticketBytes: Data, secret: Data) -> URL {
        URL(string: "https://\(BrowserLinkPairing.host)/pair#t=\(ticketBytes.base64URLEncodedString())&s=\(secret.base64URLEncodedString())")!
    }

    private func uint32Bytes(_ value: UInt32) -> Data {
        Data([
            UInt8((value >> 24) & 0xff),
            UInt8((value >> 16) & 0xff),
            UInt8((value >> 8) & 0xff),
            UInt8(value & 0xff),
        ])
    }
}

private extension Data {
    mutating func appendUInt32BigEndian(_ value: UInt32) {
        append(UInt8((value >> 24) & 0xff))
        append(UInt8((value >> 16) & 0xff))
        append(UInt8((value >> 8) & 0xff))
        append(UInt8(value & 0xff))
    }
}
