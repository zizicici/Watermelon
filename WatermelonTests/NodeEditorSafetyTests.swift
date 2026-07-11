import XCTest
import UIKit
@testable import Watermelon

final class NodeEditorSafetyTests: XCTestCase {
    override func setUp() {
        super.setUp()
        AppRuntimeFlags._testReset()
    }

    override func tearDown() {
        AppRuntimeFlags._testReset()
        super.tearDown()
    }

    func testProfileMutationLeaseBlocksExecutionStart() throws {
        let flags = AppRuntimeFlags()

        let result = flags.withProfileMutationLease(profileID: 7) {
            XCTAssertFalse(flags.tryEnterExecution())
            return 42
        }

        XCTAssertEqual(result, 42)
        XCTAssertTrue(flags.tryEnterExecution())
        flags.exitExecution()
    }

    func testExecutionBlocksProfileMutationLease() {
        let flags = AppRuntimeFlags()
        XCTAssertTrue(flags.tryEnterExecution())

        XCTAssertNil(flags.withProfileMutationLease(profileID: 7) { true })

        flags.exitExecution()
        XCTAssertEqual(flags.withProfileMutationLease(profileID: 7) { true }, true)
    }

    func testAsyncProfileMutationLeaseBlocksExecutionAndAllowsNestedCommit() async {
        let flags = AppRuntimeFlags()
        let otherFlags = AppRuntimeFlags()

        let result = await flags.withAsyncProfileMutationLease(profileID: 7) {
            XCTAssertFalse(otherFlags.tryEnterExecution())
            XCTAssertEqual(flags.withProfileMutationLease(profileID: 7) { 42 }, 42)
            await Task.yield()
            XCTAssertFalse(otherFlags.tryEnterExecution())
            return 7
        }

        XCTAssertEqual(result, 7)
        XCTAssertTrue(otherFlags.tryEnterExecution())
        otherFlags.exitExecution()
    }

    func testConnectingProfileBlocksItsMutationButNotAnotherProfile() throws {
        let flags = AppRuntimeFlags()
        let otherFlags = AppRuntimeFlags()
        XCTAssertTrue(flags.tryBeginConnecting(profileID: 7))
        XCTAssertFalse(otherFlags.tryBeginConnecting(profileID: 8))

        let blocked = flags.withProfileMutationLease(profileID: 7) { true }
        let allowed = flags.withProfileMutationLease(profileID: 8) { true }

        XCTAssertNil(blocked)
        XCTAssertEqual(allowed, true)
        flags.endConnecting(profileID: 7)
        XCTAssertTrue(otherFlags.tryBeginConnecting(profileID: 8))
        otherFlags.endConnecting(profileID: 8)
        XCTAssertEqual(flags.withProfileMutationLease(profileID: 7) { true }, true)
    }

    func testConnectingBlocksExecutionStart() {
        let flags = AppRuntimeFlags()
        XCTAssertTrue(flags.tryBeginConnecting(profileID: 7))
        XCTAssertFalse(flags.tryEnterExecution())
        flags.endConnecting(profileID: 7)
        XCTAssertTrue(flags.tryEnterExecution())
        flags.exitExecution()
    }

    func testConnectingOwnershipIsReleasedOnDeinit() {
        var owner: AppRuntimeFlags? = AppRuntimeFlags()
        XCTAssertTrue(owner?.tryBeginConnecting(profileID: 7) == true)

        owner = nil

        let next = AppRuntimeFlags()
        XCTAssertTrue(next.tryBeginConnecting(profileID: 8))
        next.endConnecting(profileID: 8)
    }

    func testRemoteDestinationComparisonIgnoresCredentialAndSettings() {
        let original = makeSMBProfile(basePath: "/A", credentialRef: "legacy", thumbnails: true)
        var edited = original
        edited.credentialRef = "path-scoped"
        edited.backgroundBackupEnabled = false
        edited.generateRemoteThumbnails = false
        edited.updatedAt = Date()

        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.basePath = "/B"
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
    }

    func testRemoteHostIdentityIsCaseInsensitiveForExistingProfiles() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var original = makeSMBProfile(basePath: "/A", credentialRef: "legacy", thumbnails: false)
        original.host = "SMB://NAS.Local/"
        original.domain = "WORKGROUP"
        try database.saveServerProfile(&original)

        var sameDestination = original
        sameDestination.host = "nas.local"
        sameDestination.shareName = "photos"
        sameDestination.basePath = "/A/"
        sameDestination.domain = "workgroup"
        XCTAssertTrue(original.hasSameRemoteDestination(as: sameDestination))
        XCTAssertEqual(RemoteHostIdentity.canonicalSMB(original.host), "nas.local")
        XCTAssertEqual(original.storageProfile.displaySubtitle, "SMB://nas.local/Photos/A")
        XCTAssertEqual(
            try database.findServerProfile(
                host: "nas.local",
                port: original.port,
                shareName: "photos",
                basePath: "/A/",
                username: original.username,
                domain: "workgroup"
            )?.id,
            original.id
        )

        var duplicate = sameDestination
        duplicate.id = nil
        XCTAssertThrowsError(try database.saveConnectionProfile(&duplicate, editingProfileID: nil))
        XCTAssertEqual(try database.fetchServerProfiles().count, 1)
    }

    func testRemoteStorageWriteVerifierRemovesProbeDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        try await RemoteStorageWriteVerifier.verify(
            client: client,
            cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
            basePath: "/target"
        )

        let entries = try await client.list(path: "/target")
        let uploadedCount = await client.uploadedPaths.count
        let createdDirectoryCount = await client.createdDirectories.count
        let deletedCount = await client.deletedPaths.count
        XCTAssertTrue(entries.isEmpty)
        XCTAssertEqual(uploadedCount, 1)
        XCTAssertEqual(createdDirectoryCount, 2)
        XCTAssertEqual(deletedCount, 2)
    }

    func testRemoteStorageWriteVerifierRejectsCorruptReadBackAndCleansProbe() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueDownloadData(Data("corrupt".utf8))

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target"
            )
            XCTFail("Expected read-back mismatch")
        } catch {
            XCTAssertTrue(error is RemoteStorageClientError)
        }

        try await waitForProbeCleanup(client)
        let entries = try await client.list(path: "/target")
        let deletedCount = await client.deletedPaths.count
        XCTAssertTrue(entries.isEmpty)
        XCTAssertEqual(deletedCount, 2)
    }

    func testRemoteStorageWriteVerifierRejectsBackendThatOverwritesConditionalCreate() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setIgnoreCreateIfAbsent(true)

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target",
                timeout: 5
            )
            XCTFail("Expected conditional-create verification to fail")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .terminal)
            guard let storageError = error as? RemoteStorageClientError,
                  case .unsafeConditionalCreateUnsupported = storageError else {
                return XCTFail("Expected explicit unsafe conditional-create error, got \(error)")
            }
            XCTAssertEqual(
                UserFacingErrorLocalizer.message(for: error, storageType: .s3),
                String(localized: "storage.client.unsafeConditionalCreateUnsupported")
            )
        }

        try await waitForProbeCleanup(client)
        let entries = try await client.list(path: "/target")
        XCTAssertTrue(entries.isEmpty)
    }

    func testRemoteStorageWriteVerifierDeadlineDoesNotWaitForUncooperativeUpload() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setOnUpload {
            await withCheckedContinuation { continuation in
                DispatchQueue.global().asyncAfter(deadline: .now() + 0.3) {
                    continuation.resume()
                }
            }
        }

        let start = Date()
        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target",
                timeout: 0.03,
                cleanupRetryDelays: [0, 0.1, 0.1]
            )
            XCTFail("Expected verifier deadline")
        } catch {
            XCTAssertLessThan(Date().timeIntervalSince(start), 0.2)
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        try await Task.sleep(nanoseconds: 500_000_000)
        let entries = try await client.list(path: "/target")
        let deletedCount = await client.deletedPaths.count
        XCTAssertTrue(entries.isEmpty)
        XCTAssertGreaterThanOrEqual(deletedCount, 2)
    }

    func testRemoteStorageWriteVerifierConfirmsCleanupAfterFailureAndLateWrite() async throws {
        let client = InMemoryRemoteStorageClient()
        let factory = NotFoundThenForwardingProbeCleanupFactory(target: client)
        await client.failUpload(
            forPathSuffix: "write-test",
            error: RemoteErrorFixtures.retryable
        )
        await client.setOnUpload {
            Task {
                while factory.count < 1 {
                    try? await Task.sleep(nanoseconds: 1_000_000)
                }
                try? await Task.sleep(nanoseconds: 20_000_000)
                guard let probeDirectory = await client.createdDirectories.last else { return }
                await client.seedFile(path: probeDirectory + "/write-test")
            }
        }

        let start = Date()
        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { factory.makeClient() },
                basePath: "/target",
                timeout: 5,
                cleanupRetryDelays: [0, 0.1]
            )
            XCTFail("Expected upload failure")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
            XCTAssertLessThan(Date().timeIntervalSince(start), 0.5)
        }

        try await waitForProbeCleanup(client, minimumFactoryCount: 0)
        let deadline = Date().addingTimeInterval(1)
        while factory.count < 2, Date() < deadline {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        let entries = try await client.list(path: "/target")
        let deletedLateWrites = await client.deletedPaths.filter { $0.hasSuffix("/write-test") }
        XCTAssertTrue(entries.isEmpty)
        XCTAssertEqual(factory.count, 2)
        XCTAssertEqual(deletedLateWrites.count, 1)
    }

    func testRemoteStorageWriteVerifierDoesNotCleanupWhenConnectFails() async throws {
        let client = ProbeStorageClient(.throwError(RemoteStorageClientError.invalidConfiguration))
        let cleanupTarget = InMemoryRemoteStorageClient()
        let factory = ProbeCleanupFactoryRecorder(target: cleanupTarget)

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { factory.makeClient() },
                basePath: "/target",
                timeout: 1,
                cleanupRetryDelays: [0, 0.01]
            )
            XCTFail("Expected connect failure")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .terminal)
        }

        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(factory.count, 0)
    }

    func testRemoteStorageWriteVerifierRetriesFreshCleanupAfterTransientFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        let factory = ProbeCleanupFactoryRecorder(target: client)
        await client.failUploadAfterWrite(
            forPathSuffix: "write-test",
            error: RemoteErrorFixtures.retryable
        )
        await client.enqueueDeleteError(RemoteErrorFixtures.retryable)

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { factory.makeClient() },
                basePath: "/target",
                timeout: 5,
                cleanupRetryDelays: [0, 0.01, 0.02]
            )
            XCTFail("Expected upload failure")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        try await waitForProbeCleanup(client, factory: factory, minimumFactoryCount: 3)
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(factory.count, 3)
    }

    func testRemoteStorageWriteVerifierReclaimsTemporaryFilesWhenOperationNeverReturns() async throws {
        let artifactsBefore = try verifierTemporaryArtifacts()
        let client = InMemoryRemoteStorageClient()
        await client.setOnUpload {
            await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
        }

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target",
                timeout: 0.03,
                cleanupRetryDelays: [0, 0.01, 0.01]
            )
            XCTFail("Expected verifier deadline")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        XCTAssertEqual(try verifierTemporaryArtifacts(), artifactsBefore)
    }

    func testRemoteStorageWriteVerifierUsesIndependentCleanupWhenWrittenUploadNeverReturns() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setOnUploadAfterWrite {
            await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
        }

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { ForwardingProbeCleanupClient(target: client) },
                basePath: "/target",
                timeout: 0.03,
                cleanupRetryDelays: [0, 0.05, 0.05]
            )
            XCTFail("Expected verifier deadline")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        try await Task.sleep(nanoseconds: 200_000_000)
        let entries = try await client.list(path: "/target")
        let deletedCount = await client.deletedPaths.count
        XCTAssertTrue(entries.isEmpty)
        XCTAssertGreaterThanOrEqual(deletedCount, 2)
    }

    func testRemoteStorageWriteVerifierDelayedCleanupRemovesLateProbeWrite() async throws {
        let client = InMemoryRemoteStorageClient()
        let factory = ProbeCleanupFactoryRecorder(target: client)
        await client.setOnUpload {
            await withCheckedContinuation { continuation in
                DispatchQueue.global().asyncAfter(deadline: .now() + 0.08) {
                    continuation.resume()
                }
            }
        }
        await client.setOnUploadAfterWrite {
            await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
        }

        do {
            try await RemoteStorageWriteVerifier.verify(
                client: client,
                cleanupClientFactory: { factory.makeClient() },
                basePath: "/target",
                timeout: 0.03,
                cleanupRetryDelays: [0, 0.12, 0.12]
            )
            XCTFail("Expected verifier deadline")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        try await Task.sleep(nanoseconds: 230_000_000)
        let entries = try await client.list(path: "/target")
        XCTAssertTrue(entries.isEmpty)
        XCTAssertGreaterThanOrEqual(factory.count, 2)
    }

    func testCredentialRefV2IsDeterministicAndUnambiguous() {
        let first = StorageProfilePersistence.credentialRef(
            storageType: .webdav,
            identityFields: ["a|b", "c"]
        )
        let same = StorageProfilePersistence.credentialRef(
            storageType: .webdav,
            identityFields: ["a|b", "c"]
        )
        let formerlyColliding = StorageProfilePersistence.credentialRef(
            storageType: .webdav,
            identityFields: ["a", "b|c"]
        )

        XCTAssertEqual(first, same)
        XCTAssertNotEqual(first, formerlyColliding)
        XCTAssertTrue(first.hasPrefix("v2|webdav|"))
    }

    func testRemoteHostCanonicalizationIsSharedAcrossBackendsAndCredentialRefs() throws {
        XCTAssertEqual(RemoteHostIdentity.canonical(" MÜNICH.Example. "), "xn--mnich-kva.example")
        XCTAssertEqual(RemoteHostIdentity.canonical("nas.local."), "nas.local")
        XCTAssertEqual(
            RemoteHostIdentity.canonical("[2001:0DB8:0:0:0:0:0:1]"),
            RemoteHostIdentity.canonical("2001:db8::1")
        )
        XCTAssertNotEqual(
            RemoteHostIdentity.canonical("[fe80::1%en0]"),
            RemoteHostIdentity.canonical("[fe80::1%en1]")
        )
        XCTAssertNotEqual(RemoteHostIdentity.canonical("nas-a.local"), RemoteHostIdentity.canonical("nas-b.local"))

        var smbUnicode = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        smbUnicode.host = "SMB://MÜNICH.Example./"
        var smbASCII = smbUnicode
        smbASCII.host = "xn--mnich-kva.example"

        var webDAVUnicode = smbUnicode
        webDAVUnicode.storageType = StorageType.webdav.rawValue
        webDAVUnicode.host = "MÜNICH.Example."
        webDAVUnicode.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        var webDAVASCII = webDAVUnicode
        webDAVASCII.host = "xn--mnich-kva.example"

        var s3Unicode = smbUnicode
        s3Unicode.storageType = StorageType.s3.rawValue
        s3Unicode.host = "MÜNICH.Example."
        s3Unicode.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )
        var s3ASCII = s3Unicode
        s3ASCII.host = "xn--mnich-kva.example"

        var sftpExpanded = smbUnicode
        sftpExpanded.storageType = StorageType.sftp.rawValue
        sftpExpanded.host = "[2001:0db8:0:0:0:0:0:1]"
        sftpExpanded.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        var sftpCompressed = sftpExpanded
        sftpCompressed.host = "2001:db8::1"

        for (first, second) in [
            (smbUnicode, smbASCII),
            (webDAVUnicode, webDAVASCII),
            (s3Unicode, s3ASCII),
            (sftpExpanded, sftpCompressed)
        ] {
            let firstDuplicate = try XCTUnwrap(first.duplicateIdentity)
            let secondDuplicate = try XCTUnwrap(second.duplicateIdentity)
            XCTAssertEqual(firstDuplicate, secondDuplicate)
            XCTAssertEqual(first.remoteDestinationIdentity, second.remoteDestinationIdentity)
            XCTAssertEqual(
                StorageProfilePersistence.credentialRef(for: firstDuplicate),
                StorageProfilePersistence.credentialRef(for: secondDuplicate)
            )
        }

        var differentHost = webDAVASCII
        differentHost.host = "other.example"
        XCTAssertNotEqual(webDAVASCII.duplicateIdentity, differentHost.duplicateIdentity)
        XCTAssertNotEqual(webDAVASCII.remoteDestinationIdentity, differentHost.remoteDestinationIdentity)

        var r2RootDot = s3ASCII
        r2RootDot.host = "account.r2.cloudflarestorage.com."
        r2RootDot.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "", usePathStyle: false)
        )
        var r2Canonical = r2RootDot
        r2Canonical.host = "account.r2.cloudflarestorage.com"
        r2Canonical.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "auto", usePathStyle: false)
        )
        XCTAssertEqual(r2RootDot.duplicateIdentity, r2Canonical.duplicateIdentity)
        XCTAssertEqual(r2RootDot.remoteDestinationIdentity, r2Canonical.remoteDestinationIdentity)

        var anotherR2 = r2Canonical
        anotherR2.host = "another.r2.cloudflarestorage.com"
        XCTAssertNotEqual(r2Canonical.duplicateIdentity, anotherR2.duplicateIdentity)
        XCTAssertNotEqual(r2Canonical.remoteDestinationIdentity, anotherR2.remoteDestinationIdentity)
    }

    func testRemoteHostEndpointSeparatesURLAuthorityFromSocketHost() throws {
        let expanded = try XCTUnwrap(RemoteHostEndpoint.representation("[2001:0DB8:0:0:0:0:0:1]"))
        XCTAssertEqual(expanded.socketHost, "2001:db8::1")
        XCTAssertEqual(expanded.urlAuthority, "[2001:db8::1]")
        XCTAssertTrue(expanded.isIPLiteral)

        let zoned = try XCTUnwrap(RemoteHostEndpoint.representation("[fe80::1%25en0]"))
        XCTAssertEqual(zoned.socketHost, "fe80::1%en0")
        XCTAssertEqual(zoned.urlAuthority, "[fe80::1%25en0]")
        XCTAssertNotEqual(
            zoned.socketHost,
            RemoteHostEndpoint.socketHost("[fe80::1%25en1]")
        )

        let smbURL = try XCTUnwrap(RemoteHostEndpoint.url(
            scheme: "smb",
            host: "smb://[2001:db8::1]/",
            port: 445,
            strippingSMBScheme: true
        ))
        XCTAssertEqual(smbURL.absoluteString, "smb://[2001:db8::1]:445")

        for host in ["nas/", "//nas/", "smb://nas/"] {
            XCTAssertEqual(
                RemoteHostEndpoint.socketHost(host, strippingSMBScheme: true),
                "nas"
            )
            XCTAssertEqual(
                RemoteHostEndpoint.url(
                    scheme: "smb",
                    host: host,
                    port: 445,
                    strippingSMBScheme: true
                )?.absoluteString,
                "smb://nas:445"
            )
        }

        let webDAVURL = try XCTUnwrap(ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: "https",
            host: "2001:db8::1",
            port: 8443,
            mountPath: "/dav"
        ))
        XCTAssertEqual(webDAVURL.absoluteString, "https://[2001:db8::1]:8443/dav")

        let rootedWebDAV = ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: "https",
            host: "MÜNICH.Example.",
            port: 443,
            mountPath: "/dav"
        )
        let canonicalWebDAV = ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: "https",
            host: "xn--mnich-kva.example",
            port: 443,
            mountPath: "/dav"
        )
        XCTAssertEqual(rootedWebDAV, canonicalWebDAV)
        XCTAssertEqual(rootedWebDAV?.absoluteString, "https://xn--mnich-kva.example/dav")
        XCTAssertNotEqual(
            rootedWebDAV,
            ServerProfileRecord.buildWebDAVEndpointURL(
                scheme: "https",
                host: "other.example",
                port: 443,
                mountPath: "/dav"
            )
        )
        XCTAssertEqual(RemoteHostEndpoint.socketHost("nas.local."), "nas.local")
    }

    func testReachabilityProbeSignatureUsesOperationalSocketHost() throws {
        var smb = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        smb.port = 0
        let smbSignatures = ["nas/", "//nas/", "smb://nas/"].map { host -> ProfileReachabilityService.ProbeSignature in
            var profile = smb
            profile.host = host
            return ProfileReachabilityService.probeSignature(of: profile)
        }
        XCTAssertEqual(Set(smbSignatures).count, 1)
        XCTAssertEqual(smbSignatures.first?.host, "nas")
        XCTAssertEqual(smbSignatures.first?.port, 445)

        var differentSMB = smb
        differentSMB.host = "other-nas"
        XCTAssertNotEqual(
            smbSignatures.first,
            ProfileReachabilityService.probeSignature(of: differentSMB)
        )

        var sftp = smb
        sftp.storageType = StorageType.sftp.rawValue
        sftp.host = "[fe80:0:0:0:0:0:0:1%25en0]"
        sftp.port = 0
        sftp.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        let sftpSignature = ProfileReachabilityService.probeSignature(of: sftp)
        XCTAssertEqual(sftpSignature.host, "fe80::1%en0")
        XCTAssertEqual(sftpSignature.port, 22)
        XCTAssertEqual(ProfileReachabilityService.operationalProbeHost(for: sftp), "fe80::1%en0")

        var invalidSMB = smb
        invalidSMB.host = "///"
        XCTAssertNil(ProfileReachabilityService.operationalProbeHost(for: invalidSMB))
        XCTAssertEqual(ProfileReachabilityService.probeSignature(of: invalidSMB).host, "")
    }

    func testSMBZeroPortIsOperationallyEquivalentToDefaultPort() throws {
        XCTAssertEqual(SMBEndpoint.effectivePort(0), SMBEndpoint.defaultPort)
        XCTAssertEqual(
            SMBEndpoint.url(host: "nas.local", port: 0),
            SMBEndpoint.url(host: "nas.local", port: SMBEndpoint.defaultPort)
        )
        XCTAssertEqual(SMBEndpoint.url(host: "nas.local", port: 0)?.absoluteString, "smb://nas.local:445")
        XCTAssertEqual(
            SMBServerLoginDraft(name: "NAS", host: "nas.local", port: 0, username: "alice", domain: nil).effectivePort,
            SMBEndpoint.defaultPort
        )

        var legacy = makeSMBProfile(basePath: "/A", credentialRef: "legacy", thumbnails: false)
        legacy.port = 0
        var canonical = legacy
        canonical.port = SMBEndpoint.defaultPort
        XCTAssertEqual(legacy.duplicateIdentity, canonical.duplicateIdentity)
        XCTAssertEqual(legacy.remoteDestinationIdentity, canonical.remoteDestinationIdentity)
        let legacyIdentity = try XCTUnwrap(legacy.duplicateIdentity)
        let canonicalIdentity = try XCTUnwrap(canonical.duplicateIdentity)
        XCTAssertEqual(
            StorageProfilePersistence.credentialRef(for: legacyIdentity),
            StorageProfilePersistence.credentialRef(for: canonicalIdentity)
        )

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        try database.saveConnectionProfile(&legacy, editingProfileID: nil)
        let profileID = try XCTUnwrap(legacy.id)

        XCTAssertEqual(
            try database.findServerProfile(
                host: legacy.host,
                port: SMBEndpoint.defaultPort,
                shareName: legacy.shareName,
                basePath: legacy.basePath,
                username: legacy.username,
                domain: legacy.domain
            )?.id,
            profileID
        )

        var duplicate = canonical
        duplicate.id = nil
        XCTAssertThrowsError(try database.saveConnectionProfile(&duplicate, editingProfileID: nil))

        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)
        canonical.id = profileID
        try database.saveConnectionProfile(&canonical, editingProfileID: profileID)

        XCTAssertEqual(try database.fetchServerProfile(id: profileID)?.port, SMBEndpoint.defaultPort)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    func testReachabilityRefreshSchedulerRunsOnlyInForeground() {
        let harness = ReachabilityRefreshSchedulerHarness()
        let scheduler = ProfileReachabilityRefreshScheduler(
            interval: 45,
            hooks: .init(
                scheduleRepeating: { interval, action in
                    harness.schedule(interval: interval, action: action)
                },
                refreshImmediately: { harness.recordImmediateRefresh() },
                refreshPeriodically: { harness.recordPeriodicRefresh() }
            )
        )

        scheduler.enterForeground()
        XCTAssertEqual(harness.immediateRefreshCount, 1)
        XCTAssertEqual(harness.scheduledIntervals, [45])
        harness.fire()
        XCTAssertEqual(harness.periodicRefreshCount, 1)

        scheduler.enterBackground()
        XCTAssertEqual(harness.cancellationCount, 1)
        harness.fire()
        XCTAssertEqual(harness.periodicRefreshCount, 1)

        scheduler.enterForeground()
        scheduler.enterForeground()
        XCTAssertEqual(harness.immediateRefreshCount, 3)
        XCTAssertEqual(harness.scheduledIntervals, [45, 45, 45])
        XCTAssertEqual(harness.cancellationCount, 2)
        harness.fire()
        XCTAssertEqual(harness.periodicRefreshCount, 2)

        scheduler.stop()
        XCTAssertEqual(harness.cancellationCount, 3)
        harness.fire()
        XCTAssertEqual(harness.periodicRefreshCount, 2)
    }

    func testReachabilityPendingForceSweepReplaysExactlyOnce() async throws {
        let harness = ManualReachabilityProbeHarness()
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let service = ProfileReachabilityService(hooks: .init(
            now: { now },
            probe: { profile, _ in await harness.probe(profile) }
        ))
        var profile = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        profile.id = 1
        service.setProfiles([profile], activeProfileID: nil)
        service.resumeForeground()
        await harness.waitForInvocationCount(1)

        service.sweep(force: false)
        service.sweep(force: true)
        service.sweep(force: false)
        _ = service.reachability(for: 1)
        let countBeforeCompletion = await harness.invocationCount
        XCTAssertEqual(countBeforeCompletion, 1)

        await harness.completeInvocation(at: 0, with: .unreachable)
        await harness.waitForInvocationCount(2)
        XCTAssertEqual(service.reachability(for: 1), .unreachable)
        await harness.completeInvocation(at: 1, with: .reachable)
        await waitForReachability(service, profileID: 1, expected: .reachable)
        let finalCount = await harness.invocationCount
        XCTAssertEqual(finalCount, 2)
    }

    func testReachabilityPendingNonforceSweepRespectsThrottle() async throws {
        let harness = ManualReachabilityProbeHarness()
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let service = ProfileReachabilityService(hooks: .init(
            now: { now },
            probe: { profile, _ in await harness.probe(profile) }
        ))
        var profile = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        profile.id = 1
        service.setProfiles([profile], activeProfileID: nil)
        service.resumeForeground()
        await harness.waitForInvocationCount(1)
        service.sweep(force: false)
        _ = service.reachability(for: 1)
        await harness.completeInvocation(at: 0, with: .unreachable)
        await waitForReachability(service, profileID: 1, expected: .unreachable)
        _ = service.reachability(for: 1)
        let finalCount = await harness.invocationCount
        XCTAssertEqual(finalCount, 1)
    }

    func testReachabilityBackgroundAndStopClearPendingSweep() async throws {
        for stopInsteadOfBackground in [false, true] {
            let harness = ManualReachabilityProbeHarness()
            let service = ProfileReachabilityService(hooks: .init(
                now: { Date(timeIntervalSince1970: 1_700_000_000) },
                probe: { profile, _ in await harness.probe(profile) }
            ))
            var profile = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
            profile.id = 1
            service.setProfiles([profile], activeProfileID: nil)
            service.resumeForeground()
            await harness.waitForInvocationCount(1)
            service.sweep(force: true)
            _ = service.reachability(for: 1)
            if stopInsteadOfBackground {
                service.stop()
            } else {
                service.pauseForBackground()
            }
            await harness.completeInvocation(at: 0, with: .unreachable)
            for _ in 0 ..< 20 { await Task.yield() }
            let finalCount = await harness.invocationCount
            XCTAssertEqual(finalCount, 1)
        }
    }

    func testReachabilityProfileChangeDoesNotReplayOldPendingSweep() async throws {
        let harness = ManualReachabilityProbeHarness()
        let service = ProfileReachabilityService(hooks: .init(
            now: { Date(timeIntervalSince1970: 1_700_000_000) },
            probe: { profile, _ in await harness.probe(profile) }
        ))
        var profile = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        profile.id = 1
        service.setProfiles([profile], activeProfileID: nil)
        service.resumeForeground()
        await harness.waitForInvocationCount(1)
        service.sweep(force: true)
        _ = service.reachability(for: 1)

        profile.host = "replacement.local"
        service.setProfiles([profile], activeProfileID: nil)
        await harness.waitForInvocationCount(2)
        let replacementHost = await harness.host(at: 1)
        XCTAssertEqual(replacementHost, "replacement.local")
        await harness.completeInvocation(at: 0, with: .unreachable)
        await harness.completeInvocation(at: 1, with: .reachable)
        await waitForReachability(service, profileID: 1, expected: .reachable)
        for _ in 0 ..< 20 { await Task.yield() }
        let finalCount = await harness.invocationCount
        XCTAssertEqual(finalCount, 2)
    }

    func testWebDAVRemoteDestinationIncludesSchemeAndPaths() throws {
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        original.storageType = StorageType.webdav.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        original.shareName = "/dav"

        var edited = original
        edited.credentialRef = "new-ref"
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "http"))
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
    }

    func testWebDAVBasePathCanonicalizationMatchesIdentityCredentialAndRequestURL() throws {
        var repeated = makeSMBProfile(basePath: "/photos//library/", credentialRef: "first", thumbnails: false)
        repeated.storageType = StorageType.webdav.rawValue
        repeated.shareName = "/dav//mount"
        repeated.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        var canonical = repeated
        canonical.basePath = "/photos/library"

        let repeatedIdentity = try XCTUnwrap(repeated.duplicateIdentity)
        let canonicalIdentity = try XCTUnwrap(canonical.duplicateIdentity)
        XCTAssertEqual(repeatedIdentity, canonicalIdentity)
        XCTAssertEqual(repeated.remoteDestinationIdentity, canonical.remoteDestinationIdentity)
        XCTAssertEqual(
            StorageProfilePersistence.credentialRef(for: repeatedIdentity),
            StorageProfilePersistence.credentialRef(for: canonicalIdentity)
        )

        let endpoint = try XCTUnwrap(URL(string: "https://example.test/dav//mount"))
        let repeatedURL = try WebDAVClient.operationalRequestURL(endpointURL: endpoint, remotePath: repeated.basePath)
        let canonicalURL = try WebDAVClient.operationalRequestURL(endpointURL: endpoint, remotePath: canonical.basePath)
        XCTAssertEqual(repeatedURL, canonicalURL)
        XCTAssertEqual(repeatedURL.absoluteString, "https://example.test/dav//mount/photos/library")

        XCTAssertEqual(try WebDAVPathCanonicalizer.canonicalRawPath("/photos/%2fslot"), "/photos/%2fslot")
        var lowercaseEscape = canonical
        lowercaseEscape.basePath = "/photos/%2fslot"
        var uppercaseEscape = canonical
        uppercaseEscape.basePath = "/photos/%2Fslot"
        let lowercaseIdentity = try XCTUnwrap(lowercaseEscape.duplicateIdentity)
        let uppercaseIdentity = try XCTUnwrap(uppercaseEscape.duplicateIdentity)
        XCTAssertNotEqual(lowercaseIdentity, uppercaseIdentity)
        XCTAssertNotEqual(
            StorageProfilePersistence.credentialRef(for: lowercaseIdentity),
            StorageProfilePersistence.credentialRef(for: uppercaseIdentity)
        )
        XCTAssertNotEqual(
            try WebDAVClient.operationalRequestURL(endpointURL: endpoint, remotePath: "/photos/%2fslot"),
            try WebDAVClient.operationalRequestURL(endpointURL: endpoint, remotePath: "/photos/%2Fslot")
        )
    }

    func testWebDAVRawPathEncodingAndHrefRoundTrip() throws {
        let cases: [(raw: String, encoded: String)] = [
            ("/archive/%20", "/archive/%2520"),
            ("/archive/%25", "/archive/%2525"),
            ("/archive/%252F", "/archive/%25252F"),
            ("/archive/%2F", "/archive/%252F"),
            ("/archive/literal space", "/archive/literal%20space"),
            ("/archive/相册", "/archive/%E7%9B%B8%E5%86%8C"),
            ("/archive/bad%zz", "/archive/bad%25zz")
        ]
        let endpoint = try XCTUnwrap(URL(string: "https://example.test/dav"))
        for value in cases {
            XCTAssertEqual(
                try WebDAVPathCanonicalizer.percentEncodedRequestPath(fromRawPath: value.raw),
                value.encoded
            )
            XCTAssertEqual(
                try WebDAVPathCanonicalizer.rawPath(fromPercentEncodedHrefPath: value.encoded),
                value.raw
            )
            let url = try WebDAVClient.operationalRequestURL(endpointURL: endpoint, remotePath: value.raw)
            XCTAssertEqual(URLComponents(url: url, resolvingAgainstBaseURL: false)?.percentEncodedPath, "/dav" + value.encoded)
        }
    }

    func testWebDAVBasePathRejectsRawDotSegmentsAndKeepsComponentsDistinct() throws {
        for invalidPath in ["/photos/./library", "/photos/../library"] {
            XCTAssertThrowsError(try WebDAVPathCanonicalizer.canonicalRawPath(invalidPath))
            XCTAssertThrowsError(
                try WebDAVClient.operationalRequestURL(
                    endpointURL: XCTUnwrap(URL(string: "https://example.test/dav")),
                    remotePath: invalidPath
                )
            )
        }

        XCTAssertEqual(
            try WebDAVPathCanonicalizer.canonicalRawPath("/photos/%2e/library"),
            "/photos/%2e/library"
        )
        XCTAssertEqual(
            try WebDAVPathCanonicalizer.percentEncodedRequestPath(fromRawPath: "/photos/%2e/library"),
            "/photos/%252e/library"
        )
        XCTAssertThrowsError(
            try WebDAVPathCanonicalizer.rawPath(fromPercentEncodedHrefPath: "/photos/%2e/library")
        )

        var invalid = makeSMBProfile(basePath: "/photos/../library", credentialRef: "invalid", thumbnails: false)
        invalid.id = 7
        invalid.storageType = StorageType.webdav.rawValue
        invalid.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        var valid = invalid
        valid.id = 8
        valid.basePath = "/photos/library"
        XCTAssertNil(invalid.duplicateIdentity)
        XCTAssertNotEqual(invalid.remoteDestinationIdentity, valid.remoteDestinationIdentity)

        var encodedSlash = valid
        encodedSlash.basePath = "/photos/a%2Fb"
        var separateComponents = valid
        separateComponents.basePath = "/photos/a/b"
        XCTAssertNotEqual(encodedSlash.duplicateIdentity, separateComponents.duplicateIdentity)
        XCTAssertNotEqual(encodedSlash.remoteDestinationIdentity, separateComponents.remoteDestinationIdentity)
    }

    func testEquivalentWebDAVBasePathEditPersistsCanonicalPathWithoutClearingState() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var original = makeSMBProfile(basePath: "/photos//library", credentialRef: "webdav", thumbnails: false)
        original.storageType = StorageType.webdav.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        try database.saveServerProfile(&original)
        let profileID = try XCTUnwrap(original.id)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)

        var edited = original
        edited.basePath = try WebDAVPathCanonicalizer.canonicalRawPath(original.basePath)
        try database.saveConnectionProfile(&edited, editingProfileID: profileID)

        XCTAssertEqual(try database.fetchServerProfile(id: profileID)?.basePath, "/photos/library")
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
    }

    func testS3RemoteDestinationIncludesSigningConfiguration() throws {
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        original.storageType = StorageType.s3.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )

        var edited = original
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: false)
        )
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
    }

    func testDuplicateIdentityMatchesEffectiveBackendRoutes() throws {
        var webDAVLegacy = makeSMBProfile(basePath: "/A", credentialRef: "webdav", thumbnails: false)
        webDAVLegacy.storageType = StorageType.webdav.rawValue
        webDAVLegacy.port = 0
        webDAVLegacy.shareName = "/dav"
        webDAVLegacy.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        var webDAVExplicit = webDAVLegacy
        webDAVExplicit.port = 443
        XCTAssertEqual(webDAVLegacy.duplicateIdentity, webDAVExplicit.duplicateIdentity)

        var s3Legacy = makeSMBProfile(basePath: "/photos", credentialRef: "s3", thumbnails: false)
        s3Legacy.storageType = StorageType.s3.rawValue
        s3Legacy.host = "account.r2.cloudflarestorage.com"
        s3Legacy.port = 0
        s3Legacy.shareName = "bucket"
        s3Legacy.username = "access-key"
        s3Legacy.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "", usePathStyle: true)
        )
        var s3Resolved = s3Legacy
        s3Resolved.port = 443
        s3Resolved.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "auto", usePathStyle: true)
        )
        XCTAssertEqual(s3Legacy.duplicateIdentity, s3Resolved.duplicateIdentity)

        var customS3Default = s3Legacy
        customS3Default.host = "objects.example.test"
        customS3Default.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "", usePathStyle: true)
        )
        var customS3Explicit = customS3Default
        customS3Explicit.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )
        XCTAssertEqual(S3Client.effectiveSigningRegion(userInput: "", host: customS3Default.host), "us-east-1")
        XCTAssertEqual(customS3Default.duplicateIdentity, customS3Explicit.duplicateIdentity)
        XCTAssertEqual(customS3Default.remoteDestinationIdentity, customS3Explicit.remoteDestinationIdentity)

        var sftp = makeSMBProfile(basePath: "/photos", credentialRef: "sftp", thumbnails: false)
        sftp.storageType = StorageType.sftp.rawValue
        sftp.port = 0
        sftp.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old")
        )
        var changedFingerprint = sftp
        changedFingerprint.port = 22
        changedFingerprint.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .privateKey, hostKeyFingerprintSHA256: "new")
        )
        XCTAssertEqual(sftp.duplicateIdentity, changedFingerprint.duplicateIdentity)
    }

    func testDatabaseRejectsCanonicalDuplicatesForStructuredNetworkBackends() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var webDAV = makeSMBProfile(basePath: "/A//library/", credentialRef: "webdav-a", thumbnails: false)
        webDAV.storageType = StorageType.webdav.rawValue
        webDAV.port = 0
        webDAV.connectionParams = try ServerProfileRecord.encodedConnectionParams(WebDAVConnectionParams(scheme: "https"))
        try database.saveConnectionProfile(&webDAV, editingProfileID: nil)
        var webDAVDuplicate = webDAV
        webDAVDuplicate.id = nil
        webDAVDuplicate.port = 443
        webDAVDuplicate.basePath = "/A/library"
        webDAVDuplicate.credentialRef = "webdav-b"
        XCTAssertThrowsError(try database.saveConnectionProfile(&webDAVDuplicate, editingProfileID: nil))

        var s3 = makeSMBProfile(basePath: "/photos", credentialRef: "s3-a", thumbnails: false)
        s3.storageType = StorageType.s3.rawValue
        s3.host = "objects.example.test"
        s3.port = 0
        s3.shareName = "bucket"
        s3.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "", usePathStyle: true)
        )
        try database.saveConnectionProfile(&s3, editingProfileID: nil)
        var s3Duplicate = s3
        s3Duplicate.id = nil
        s3Duplicate.port = 443
        s3Duplicate.credentialRef = "s3-b"
        s3Duplicate.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )
        XCTAssertThrowsError(try database.saveConnectionProfile(&s3Duplicate, editingProfileID: nil))

        var sftp = makeSMBProfile(basePath: "/photos", credentialRef: "sftp-a", thumbnails: false)
        sftp.storageType = StorageType.sftp.rawValue
        sftp.port = 0
        sftp.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old")
        )
        try database.saveConnectionProfile(&sftp, editingProfileID: nil)
        var sftpDuplicate = sftp
        sftpDuplicate.id = nil
        sftpDuplicate.port = 22
        sftpDuplicate.credentialRef = "sftp-b"
        sftpDuplicate.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .privateKey, hostKeyFingerprintSHA256: "new")
        )
        XCTAssertThrowsError(try database.saveConnectionProfile(&sftpDuplicate, editingProfileID: nil))

    }

    func testSFTPRemoteDestinationIgnoresCredentialModeButIncludesHostKey() throws {
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        original.storageType = StorageType.sftp.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old")
        )

        var edited = original
        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .privateKey, hostKeyFingerprintSHA256: "old")
        )
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .privateKey, hostKeyFingerprintSHA256: "new")
        )
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))

        edited.connectionParams = original.connectionParams
        edited.port = 2222
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
    }

    func testSFTPLegacyDefaultPortUsesChangedKeyWarningPolicy() {
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.decision(
                existingHost: "NAS.Local.",
                existingPort: 0,
                expectedFingerprint: "old-key",
                proposedHost: "nas.local",
                proposedPort: 22,
                actualFingerprint: "new-key"
            ),
            .changedKey(expected: "old-key")
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.decision(
                existingHost: "nas.local",
                existingPort: 0,
                expectedFingerprint: "old-key",
                proposedHost: "nas.local",
                proposedPort: 2222,
                actualFingerprint: "new-key"
            ),
            .firstTrust
        )
        XCTAssertEqual(
            SFTPHostKeyPromptPolicy.decision(
                existingHost: "nas.local",
                existingPort: 0,
                expectedFingerprint: "same-key",
                proposedHost: "nas.local",
                proposedPort: 22,
                actualFingerprint: "same-key"
            ),
            .none
        )
    }

    func testSFTPEffectivePortIsSharedByIdentityClientReachabilityAndDisplay() throws {
        var legacy = makeSMBProfile(basePath: "/photos", credentialRef: "legacy", thumbnails: false)
        legacy.storageType = StorageType.sftp.rawValue
        legacy.host = "NAS.Local."
        legacy.port = 0
        legacy.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        var explicit = legacy
        explicit.host = "nas.local"
        explicit.port = 22

        let legacyIdentity = try XCTUnwrap(legacy.duplicateIdentity)
        let explicitIdentity = try XCTUnwrap(explicit.duplicateIdentity)
        XCTAssertEqual(SFTPEndpoint.effectivePort(legacy.port), SFTPEndpoint.defaultPort)
        XCTAssertEqual(legacyIdentity, explicitIdentity)
        XCTAssertEqual(legacy.remoteDestinationIdentity, explicit.remoteDestinationIdentity)
        XCTAssertEqual(
            StorageProfilePersistence.credentialRef(for: legacyIdentity),
            StorageProfilePersistence.credentialRef(for: explicitIdentity)
        )
        XCTAssertEqual(legacy.sftpDisplayURLString, explicit.sftpDisplayURLString)

        let legacySignature = ProfileReachabilityService.probeSignature(of: legacy)
        let explicitSignature = ProfileReachabilityService.probeSignature(of: explicit)
        XCTAssertEqual(legacySignature, explicitSignature)
        XCTAssertEqual(legacySignature.port, SFTPEndpoint.defaultPort)

        let credential = SFTPCredentialBlob.password("secret")
        let legacyConfig = SFTPClient.Config(
            host: legacy.host,
            port: legacy.port,
            username: legacy.username,
            credential: credential,
            expectedHostKeyFingerprintSHA256: "host-key"
        )
        let explicitConfig = SFTPClient.Config(
            host: explicit.host,
            port: explicit.port,
            username: explicit.username,
            credential: credential,
            expectedHostKeyFingerprintSHA256: "host-key"
        )
        XCTAssertEqual(legacyConfig.effectivePort, explicitConfig.effectivePort)
    }

    func testSFTPLegacyDefaultPortEditCanonicalizesWithoutClearingState() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var legacy = makeSMBProfile(basePath: "/photos", credentialRef: "", thumbnails: false)
        legacy.storageType = StorageType.sftp.rawValue
        legacy.port = 0
        legacy.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "host-key")
        )
        legacy.credentialRef = StorageProfilePersistence.credentialRef(for: try XCTUnwrap(legacy.duplicateIdentity))
        try database.saveServerProfile(&legacy)
        let profileID = try XCTUnwrap(legacy.id)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        var edited = legacy
        edited.port = SFTPEndpoint.defaultPort
        edited.credentialRef = StorageProfilePersistence.credentialRef(for: try XCTUnwrap(edited.duplicateIdentity))
        XCTAssertEqual(legacy.credentialRef, edited.credentialRef)
        XCTAssertTrue(legacy.hasSameRemoteDestination(as: edited))
        try database.saveConnectionProfile(&edited, editingProfileID: profileID)

        XCTAssertEqual(try database.fetchServerProfile(id: profileID)?.port, SFTPEndpoint.defaultPort)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    func testExternalRemoteDestinationUsesStableLocationToken() throws {
        var original = makeSMBProfile(basePath: "/", credentialRef: "ref", thumbnails: false)
        original.storageType = StorageType.externalVolume.rawValue
        original.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([1]), displayPath: "/Volumes/Photos")
        )

        var edited = original
        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([2]), displayPath: "/Volumes/Photos")
        )
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([2]), displayPath: "/Volumes/Archive")
        )
        XCTAssertTrue(original.hasSameRemoteDestination(as: edited))

        edited.shareName = "external-new-location"
        XCTAssertFalse(original.hasSameRemoteDestination(as: edited))
    }

    func testExternalLocationIdentityIsEphemeralAndNeverPersisted() throws {
        let first = SecurityScopedBookmarkStore.ephemeralLocationIdentity(
            volumeIdentifier: Data([1, 2]),
            fileResourceIdentifier: Data([3, 4])
        )
        let same = SecurityScopedBookmarkStore.ephemeralLocationIdentity(
            volumeIdentifier: Data([1, 2]),
            fileResourceIdentifier: Data([3, 4])
        )
        let different = SecurityScopedBookmarkStore.ephemeralLocationIdentity(
            volumeIdentifier: Data([1, 2]),
            fileResourceIdentifier: Data([3, 5])
        )
        XCTAssertEqual(first, same)
        XCTAssertNotEqual(first, different)

        let legacyJSON = try JSONSerialization.data(withJSONObject: [
            "rootBookmarkData": Data([1]).base64EncodedString(),
            "displayPath": "/Volumes/Photos",
            "locationIdentity": "previous-unreleased-value"
        ])
        let legacy = try JSONDecoder().decode(ExternalVolumeConnectionParams.self, from: legacyJSON)
        XCTAssertEqual(legacy.displayPath, "/Volumes/Photos")
        let reencoded = try ServerProfileRecord.encodedConnectionParams(legacy)
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: reencoded) as? [String: Any])
        XCTAssertNil(object["locationIdentity"])

        var profile = makeSMBProfile(basePath: "/", credentialRef: "external", thumbnails: false)
        profile.storageType = StorageType.externalVolume.rawValue
        profile.connectionParams = reencoded
        XCTAssertNil(profile.duplicateIdentity)
    }

    func testExternalRepickUsesCurrentEphemeralIdentityThenResolvedURLFallback() {
        let original = ExternalVolumeCurrentLocation(
            ephemeralIdentity: Data("resource-a".utf8),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Old")
        )
        let renamed = ExternalVolumeCurrentLocation(
            ephemeralIdentity: Data("resource-a".utf8),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Renamed")
        )
        let replacedAtSamePath = ExternalVolumeCurrentLocation(
            ephemeralIdentity: Data("new-mount-resource".utf8),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Old")
        )
        let identityUnavailable = ExternalVolumeCurrentLocation(
            ephemeralIdentity: nil,
            standardizedURL: URL(fileURLWithPath: "/Volumes/Old")
        )
        let different = ExternalVolumeCurrentLocation(
            ephemeralIdentity: Data("resource-b".utf8),
            standardizedURL: URL(fileURLWithPath: "/Volumes/Other")
        )
        XCTAssertTrue(ExternalVolumeLocationPolicy.representsSameLocation(original, renamed))
        XCTAssertFalse(ExternalVolumeLocationPolicy.representsSameLocation(original, replacedAtSamePath))
        XCTAssertTrue(ExternalVolumeLocationPolicy.representsSameLocation(original, identityUnavailable))
        XCTAssertFalse(ExternalVolumeLocationPolicy.representsSameLocation(original, different))
        XCTAssertTrue(ExternalVolumeLocationPolicy.containsDuplicate(
            candidate: original,
            existingLocations: [different, renamed]
        ))
        XCTAssertFalse(ExternalVolumeLocationPolicy.containsDuplicate(
            candidate: original,
            existingLocations: [different]
        ))
        XCTAssertEqual(ExternalVolumeLocationPolicy.locationToken(
            existingToken: "external-a",
            selectedNewLocation: true,
            existingLocation: renamed,
            candidateLocation: original,
            makeToken: { "external-new" }
        ), "external-a")
        XCTAssertEqual(ExternalVolumeLocationPolicy.locationToken(
            existingToken: "external-a",
            selectedNewLocation: true,
            existingLocation: different,
            candidateLocation: original,
            makeToken: { "external-new" }
        ), "external-new")
        XCTAssertEqual(ExternalVolumeLocationPolicy.locationToken(
            existingToken: "external-a",
            selectedNewLocation: true,
            existingLocation: original,
            candidateLocation: replacedAtSamePath,
            makeToken: { "external-new" }
        ), "external-new")
    }

    func testExternalBookmarkAndPathRefreshKeepRemoteProfileKeyStable() throws {
        var legacy = makeSMBProfile(basePath: "/", credentialRef: "external-ref", thumbnails: false)
        legacy.id = 7
        legacy.storageType = StorageType.externalVolume.rawValue
        legacy.shareName = "external-location-token"
        legacy.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([1]), displayPath: "/Volumes/Old")
        )
        var refreshed = legacy
        refreshed.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data([2]),
                displayPath: "/Volumes/Renamed"
            )
        )
        XCTAssertEqual(legacy.remoteDestinationIdentity, refreshed.remoteDestinationIdentity)
        XCTAssertEqual(
            RemoteIndexSyncService.remoteProfileKey(legacy),
            RemoteIndexSyncService.remoteProfileKey(refreshed)
        )
    }

    func testRemoteProfileKeyUsesCanonicalDestinationIdentityAndProfileID() throws {
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        original.id = 7
        original.host = "SMB://NAS.Local/"
        original.domain = "WORKGROUP"

        var canonicalEdit = original
        canonicalEdit.host = "nas.local"
        canonicalEdit.shareName = "photos"
        canonicalEdit.basePath = "/A/"
        canonicalEdit.domain = "workgroup"
        XCTAssertEqual(
            RemoteIndexSyncService.remoteProfileKey(original),
            RemoteIndexSyncService.remoteProfileKey(canonicalEdit)
        )

        var anotherProfile = canonicalEdit
        anotherProfile.id = 8
        XCTAssertNotEqual(
            RemoteIndexSyncService.remoteProfileKey(original),
            RemoteIndexSyncService.remoteProfileKey(anotherProfile)
        )

        var sftp = original
        sftp.storageType = StorageType.sftp.rawValue
        sftp.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "old")
        )
        var changedHostKey = sftp
        changedHostKey.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: "new")
        )
        XCTAssertNotEqual(
            RemoteIndexSyncService.remoteProfileKey(sftp),
            RemoteIndexSyncService.remoteProfileKey(changedHostKey)
        )
    }

    func testInvalidRemoteDestinationIdentityFailsClosed() {
        var first = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        first.id = 7
        first.storageType = StorageType.webdav.rawValue
        first.connectionParams = nil
        var changed = first
        changed.host = "other.local"

        XCTAssertFalse(first.hasSameRemoteDestination(as: changed))
        XCTAssertNotEqual(
            RemoteIndexSyncService.remoteProfileKey(first),
            RemoteIndexSyncService.remoteProfileKey(changed)
        )
    }

    func testSFTPHostKeyCaptureHasHardDeadline() async {
        let start = Date()
        do {
            _ = try await SFTPClient.captureHostKeyFingerprint(host: "127.0.0.1", port: 9, timeout: 0)
            XCTFail("Expected capture deadline")
        } catch {
            XCTAssertLessThan(Date().timeIntervalSince(start), 1)
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }
    }

    func testExternalBookmarkRefreshUsesTargetedCompareAndSwap() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        let oldParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([1]), displayPath: "/Volumes/Old")
        )
        let refreshedParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data([2]),
                displayPath: "/Volumes/New"
            )
        )
        let conflictingParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(rootBookmarkData: Data([3]), displayPath: "/Volumes/Other")
        )
        var profile = makeSMBProfile(basePath: "/", credentialRef: "external-ref", thumbnails: false)
        profile.storageType = StorageType.externalVolume.rawValue
        profile.connectionParams = oldParams
        try database.saveServerProfile(&profile)
        let profileID = try XCTUnwrap(profile.id)
        let profileKeyBeforeRefresh = RemoteIndexSyncService.remoteProfileKey(profile)
        try database.setServerProfileName("Live Name", profileID: profileID)
        try database.setBackgroundBackupEnabled(false, profileID: profileID)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        XCTAssertTrue(try database.refreshExternalVolumeConnectionParams(
            profileID: profileID,
            expectedConnectionParams: oldParams,
            refreshedConnectionParams: refreshedParams
        ))
        let refreshed = try XCTUnwrap(database.fetchServerProfile(id: profileID))
        XCTAssertEqual(refreshed.name, "Live Name")
        XCTAssertFalse(refreshed.backgroundBackupEnabled)
        XCTAssertEqual(refreshed.connectionParams, refreshedParams)
        XCTAssertEqual(RemoteIndexSyncService.remoteProfileKey(refreshed), profileKeyBeforeRefresh)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))
        XCTAssertTrue(database.matchesAcceptedExternalBookmarkRefresh(
            profileID: profileID,
            previousConnectionParams: oldParams,
            currentConnectionParams: refreshedParams
        ))

        XCTAssertFalse(try database.refreshExternalVolumeConnectionParams(
            profileID: profileID,
            expectedConnectionParams: oldParams,
            refreshedConnectionParams: conflictingParams
        ))
        XCTAssertEqual(try database.fetchServerProfile(id: profileID)?.connectionParams, refreshedParams)
    }

    func testDeletingProfileClearsPersistedActiveProfileID() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var profile = makeSMBProfile(basePath: "/A", credentialRef: "ref", thumbnails: false)
        try database.saveServerProfile(&profile)
        let profileID = try XCTUnwrap(profile.id)
        try database.setActiveServerProfileID(profileID)

        try database.deleteServerProfile(id: profileID)

        XCTAssertNil(try database.activeServerProfileID())
        XCTAssertTrue(try database.fetchServerProfiles().isEmpty)
    }

    func testConnectionEditPreservesLiveMetadataAndInvalidatesDestinationStateAtomically() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var original = makeSMBProfile(basePath: "/A", credentialRef: "ref-a", thumbnails: false)
        try database.saveServerProfile(&original)
        let profileID = try XCTUnwrap(original.id)
        let writerID = try XCTUnwrap(original.writerID)

        try database.setServerProfileName("Live Name", profileID: profileID)
        try database.setBackgroundBackupEnabled(false, profileID: profileID)
        try database.setBackgroundBackupMinIntervalMinutes(180, profileID: profileID)
        try database.setBackgroundBackupRequiresWiFi(true, profileID: profileID)
        try database.setGenerateRemoteThumbnails(true, profileID: profileID)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        var edited = original
        edited.basePath = "/B"
        edited.credentialRef = "ref-b"
        try database.saveConnectionProfile(&edited, editingProfileID: profileID)

        let saved = try XCTUnwrap(database.fetchServerProfile(id: profileID))
        XCTAssertEqual(saved.name, "Live Name")
        XCTAssertEqual(saved.basePath, "/B")
        XCTAssertFalse(saved.backgroundBackupEnabled)
        XCTAssertEqual(saved.backgroundBackupMinIntervalMinutes, 180)
        XCTAssertTrue(saved.backgroundBackupRequiresWiFi)
        XCTAssertTrue(saved.generateRemoteThumbnails)
        XCTAssertEqual(saved.writerID, writerID)
        XCTAssertNil(try database.activeServerProfileID())
        XCTAssertNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    func testS3EffectiveSigningRegionControlsCacheIdentityAndEditInvalidation() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var profile = makeSMBProfile(basePath: "/photos", credentialRef: "s3-ref", thumbnails: false)
        profile.storageType = StorageType.s3.rawValue
        profile.host = "objects.example.test"
        profile.port = 443
        profile.shareName = "bucket"
        profile.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "", usePathStyle: true)
        )
        try database.saveConnectionProfile(&profile, editingProfileID: nil)
        let profileID = try XCTUnwrap(profile.id)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        var explicitDefault = profile
        explicitDefault.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-east-1", usePathStyle: true)
        )
        XCTAssertTrue(profile.hasSameRemoteDestination(as: explicitDefault))
        XCTAssertEqual(
            RemoteIndexSyncService.remoteProfileKey(profile),
            RemoteIndexSyncService.remoteProfileKey(explicitDefault)
        )
        try database.saveConnectionProfile(&explicitDefault, editingProfileID: profileID)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))

        var changedRegion = explicitDefault
        changedRegion.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: "https", region: "us-west-2", usePathStyle: true)
        )
        XCTAssertFalse(explicitDefault.hasSameRemoteDestination(as: changedRegion))
        XCTAssertNotEqual(
            RemoteIndexSyncService.remoteProfileKey(explicitDefault),
            RemoteIndexSyncService.remoteProfileKey(changedRegion)
        )
        try database.saveConnectionProfile(&changedRegion, editingProfileID: profileID)
        XCTAssertNil(try database.activeServerProfileID())
        XCTAssertNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    func testExternalSameLocationRenewalKeepsStateButTrueRepickInvalidatesIt() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonNodeEditorTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))

        var profile = makeSMBProfile(basePath: "/", credentialRef: "external-ref", thumbnails: false)
        profile.storageType = StorageType.externalVolume.rawValue
        profile.shareName = "external-location-a"
        profile.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data([1]),
                displayPath: "/Volumes/Old"
            )
        )
        try database.saveConnectionProfile(&profile, editingProfileID: nil)
        let profileID = try XCTUnwrap(profile.id)
        try database.setActiveServerProfileID(profileID)
        try database.setRemoteVerifiedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        try database.setBackgroundBackupLastRanAt(Date(), profileID: profileID)

        var renewed = profile
        renewed.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data([2]),
                displayPath: "/Volumes/Renamed"
            )
        )
        try database.saveConnectionProfile(&renewed, editingProfileID: profileID)
        XCTAssertEqual(try database.activeServerProfileID(), profileID)
        XCTAssertNotNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNotNil(try database.backgroundBackupLastRanAt(profileID: profileID))

        var repicked = renewed
        repicked.shareName = "external-location-b"
        repicked.connectionParams = try ServerProfileRecord.encodedConnectionParams(
            ExternalVolumeConnectionParams(
                rootBookmarkData: Data([3]),
                displayPath: "/Volumes/Other"
            )
        )
        try database.saveConnectionProfile(&repicked, editingProfileID: profileID)
        XCTAssertNil(try database.activeServerProfileID())
        XCTAssertNil(try database.remoteVerifiedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastCompletedAt(profileID: profileID))
        XCTAssertNil(try database.backgroundBackupLastRanAt(profileID: profileID))
    }

    @MainActor
    func testMaskedCredentialCanBeClearedDirectly() {
        let cell = CredentialTextFieldCell(style: .default, reuseIdentifier: nil)
        cell.configure(
            title: "Password",
            text: "",
            placeholder: "",
            isMasked: true,
            isRevealed: false,
            revealAccessibilityLabel: "Reveal",
            hideAccessibilityLabel: "Hide",
            inputAccessoryView: nil
        )
        var replacement: String?
        cell.onMaskedCredentialEdited = { replacement = $0 }
        let textField = UITextField()
        textField.text = "********"

        let shouldApply = cell.textField(
            textField,
            shouldChangeCharactersIn: NSRange(location: 7, length: 1),
            replacementString: ""
        )

        XCTAssertFalse(shouldApply)
        XCTAssertEqual(replacement, "")
        XCTAssertEqual(textField.text, "")
    }

    func testSettingsFormLayoutPolicyOnlyStacksAccessibilityCategories() {
        XCTAssertFalse(SettingsFormLayoutPolicy.usesVerticalLayout(for: .large))
        XCTAssertFalse(SettingsFormLayoutPolicy.usesVerticalLayout(for: .extraExtraExtraLarge))
        XCTAssertTrue(SettingsFormLayoutPolicy.usesVerticalLayout(for: .accessibilityMedium))
        XCTAssertTrue(SettingsFormLayoutPolicy.usesVerticalLayout(for: .accessibilityExtraExtraExtraLarge))
    }

    private func makeSMBProfile(basePath: String, credentialRef: String, thumbnails: Bool) -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil,
            name: "NAS",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "nas.local",
            port: 445,
            shareName: "Photos",
            basePath: basePath,
            username: "alice",
            domain: nil,
            credentialRef: credentialRef,
            backgroundBackupEnabled: true,
            backgroundBackupMinIntervalMinutes: 720,
            backgroundBackupRequiresWiFi: false,
            generateRemoteThumbnails: thumbnails,
            createdAt: Date(timeIntervalSince1970: 1_700_000_000),
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    private func waitForReachability(
        _ service: ProfileReachabilityService,
        profileID: Int64,
        expected: ProfileReachabilityService.Reachability
    ) async {
        for _ in 0 ..< 1_000 {
            if service.reachability(for: profileID) == expected { return }
            await Task.yield()
        }
        XCTFail("Reachability did not become \(expected)")
    }

    private func verifierTemporaryArtifacts() throws -> Set<String> {
        Set(try FileManager.default.contentsOfDirectory(
            at: FileManager.default.temporaryDirectory,
            includingPropertiesForKeys: nil
        ).map(\.lastPathComponent).filter { $0.hasPrefix(".watermelon-probe-") })
    }

    private func waitForProbeCleanup(
        _ client: InMemoryRemoteStorageClient,
        factory: ProbeCleanupFactoryRecorder? = nil,
        minimumFactoryCount: Int = 0,
        timeout: TimeInterval = 2
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            let entries = try await client.list(path: "/target")
            if entries.isEmpty, (factory?.count ?? 0) >= minimumFactoryCount { return }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTFail("Probe cleanup did not finish")
    }
}

private actor ManualReachabilityProbeHarness {
    private struct Invocation {
        let host: String
        var continuation: CheckedContinuation<ProfileReachabilityService.Reachability, Never>?
    }

    private var invocations: [Invocation] = []
    private var countWaiters: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

    var invocationCount: Int { invocations.count }

    func probe(_ profile: ServerProfileRecord) async -> ProfileReachabilityService.Reachability {
        await withCheckedContinuation { continuation in
            invocations.append(Invocation(host: profile.host, continuation: continuation))
            let count = invocations.count
            let ready = countWaiters.filter { $0.count <= count }
            countWaiters.removeAll { $0.count <= count }
            ready.forEach { $0.continuation.resume() }
        }
    }

    func waitForInvocationCount(_ count: Int) async {
        if invocations.count >= count { return }
        await withCheckedContinuation { continuation in
            countWaiters.append((count, continuation))
        }
    }

    func completeInvocation(
        at index: Int,
        with result: ProfileReachabilityService.Reachability
    ) {
        guard invocations.indices.contains(index),
              let continuation = invocations[index].continuation else { return }
        invocations[index].continuation = nil
        continuation.resume(returning: result)
    }

    func host(at index: Int) -> String? {
        guard invocations.indices.contains(index) else { return nil }
        return invocations[index].host
    }
}

private final class ReachabilityRefreshSchedulerHarness: @unchecked Sendable {
    private let lock = NSLock()
    private var action: (@Sendable () -> Void)?
    private var immediateCount = 0
    private var periodicCount = 0
    private var cancellations = 0
    private var intervals: [TimeInterval] = []

    var immediateRefreshCount: Int { lock.withLock { immediateCount } }
    var periodicRefreshCount: Int { lock.withLock { periodicCount } }
    var cancellationCount: Int { lock.withLock { cancellations } }
    var scheduledIntervals: [TimeInterval] { lock.withLock { intervals } }

    func schedule(
        interval: TimeInterval,
        action: @escaping @Sendable () -> Void
    ) -> (@Sendable () -> Void) {
        lock.withLock {
            intervals.append(interval)
            self.action = action
        }
        return { [weak self] in
            self?.lock.withLock {
                self?.cancellations += 1
                self?.action = nil
            }
        }
    }

    func recordImmediateRefresh() {
        lock.withLock { immediateCount += 1 }
    }

    func recordPeriodicRefresh() {
        lock.withLock { periodicCount += 1 }
    }

    func fire() {
        let action = lock.withLock { self.action }
        action?()
    }
}

private final class ProbeCleanupFactoryRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private let target: InMemoryRemoteStorageClient
    private var createdCount = 0

    init(target: InMemoryRemoteStorageClient) {
        self.target = target
    }

    var count: Int { lock.withLock { createdCount } }

    func makeClient() -> any RemoteStorageClientProtocol {
        lock.withLock { createdCount += 1 }
        return ForwardingProbeCleanupClient(target: target)
    }
}

private final class NotFoundThenForwardingProbeCleanupFactory: @unchecked Sendable {
    private let lock = NSLock()
    private let target: InMemoryRemoteStorageClient
    private var createdCount = 0

    init(target: InMemoryRemoteStorageClient) {
        self.target = target
    }

    var count: Int { lock.withLock { createdCount } }

    func makeClient() -> any RemoteStorageClientProtocol {
        let count = lock.withLock {
            createdCount += 1
            return createdCount
        }
        if count == 1 {
            return NotFoundProbeCleanupClient()
        }
        return ForwardingProbeCleanupClient(target: target)
    }
}

private actor NotFoundProbeCleanupClient: RemoteStorageClientProtocol {
    func connect() async throws {}
    func disconnect() async {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }
    func list(path: String) async throws -> [RemoteStorageEntry] { [] }
    func metadata(path: String) async throws -> RemoteStorageEntry? { nil }
    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {}
    func setModificationDate(_ date: Date, forPath path: String) async throws {}
    func download(remotePath: String, localURL: URL) async throws {}
    func exists(path: String) async throws -> Bool { false }
    func delete(path: String) async throws { throw RemoteErrorFixtures.notFound }
    func createDirectory(path: String) async throws {}
    func move(from sourcePath: String, to destinationPath: String) async throws {}
    func copy(from sourcePath: String, to destinationPath: String) async throws {}
}

private actor ForwardingProbeCleanupClient: RemoteStorageClientProtocol {
    let target: InMemoryRemoteStorageClient

    init(target: InMemoryRemoteStorageClient) {
        self.target = target
    }

    func connect() async throws {}
    func disconnect() async {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }
    func list(path: String) async throws -> [RemoteStorageEntry] { [] }
    func metadata(path: String) async throws -> RemoteStorageEntry? { nil }
    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {}
    func setModificationDate(_ date: Date, forPath path: String) async throws {}
    func download(remotePath: String, localURL: URL) async throws {}
    func exists(path: String) async throws -> Bool { false }
    func delete(path: String) async throws { try await target.delete(path: path) }
    func createDirectory(path: String) async throws {}
    func move(from sourcePath: String, to destinationPath: String) async throws {}
    func copy(from sourcePath: String, to destinationPath: String) async throws {}
}
