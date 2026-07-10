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
    private func instance(
        fileName: String,
        resourceHash: Data = Data(),
        storageCodec: Int = RemoteManifestResource.plaintextStorageCodec,
        encryptionKeyID: String? = nil
    ) -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(
            role: 1,
            slot: 0,
            resourceHash: resourceHash,
            fileName: fileName,
            fileSize: 0,
            remoteRelativePath: "2026/06/\(fileName)",
            creationDateMs: nil,
            storageCodec: storageCodec,
            encryptionKeyID: encryptionKeyID
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
        await client.enqueueDownloadData(Data("resource-one-bytes".utf8))
        await client.enqueueDownloadError(RemoteErrorFixtures.terminal)

        let service = RestoreService(makeClient: { _, _ in client })
        let items = [
            RestoreService.RestoreItemDescriptor(
                instances: [instance(fileName: "\(token)_A.JPG"), instance(fileName: "\(token)_B.MOV")],
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

    func testEncryptedMissingKeyDoesNotPreventLaterPlaintextDownloadAttempt() async throws {
        let client = InMemoryRemoteStorageClient()
        let plainVersion = VersionManifestLite.makeManifest(createdAt: "2026-07-09T00:00:00Z", createdBy: "seed")
        await client.enqueueDownloadData(try VersionManifestLite.encode(plainVersion))
        await client.enqueueDownloadError(RemoteErrorFixtures.terminal)

        let service = RestoreService(makeClient: { _, _ in client }, encryptionKeyStore: MemoryRepoEncryptionKeyStore())
        let encrypted = RestoreService.RestoreItemDescriptor(
            instances: [
                instance(
                    fileName: "encrypted.wmenc",
                    resourceHash: Data(repeating: 0x11, count: RemoteManifestResource.contentHashByteCount),
                    storageCodec: RemoteManifestResource.encryptedStorageCodec,
                    encryptionKeyID: "missing-key"
                )
            ],
            identity: Data([0xE1])
        )
        let plaintext = RestoreService.RestoreItemDescriptor(
            instances: [instance(fileName: "plain.jpg")],
            identity: Data([0x01])
        )

        do {
            _ = try await service.restoreItems(
                items: [encrypted, plaintext],
                profile: profile(),
                password: "pw",
                onItemCompleted: { _, _, _ in }
            )
            XCTFail("expected the plaintext download fault to propagate")
        } catch {
            let nsError = error as NSError
            XCTAssertEqual(nsError.domain, "WriteLockTestTerminal")
        }

        let attempts = await client.downloadAttemptPaths
        XCTAssertTrue(
            attempts.contains { $0.hasSuffix("/2026/06/plain.jpg") },
            "plaintext restore was not attempted after the encrypted item failed: \(attempts)"
        )
    }

    func testEncryptionContextLoadRecoversOntoFreshClient() async throws {
        let initial = InMemoryRemoteStorageClient()
        await initial.enqueueDownloadError(RemoteErrorFixtures.retryable)
        let fresh = InMemoryRemoteStorageClient()
        let keyStore = MemoryRepoEncryptionKeyStore()
        let material = try RepoEncryptionKeyMaterial(
            repoID: "repo-restore-recovery",
            keyID: "key-restore-recovery",
            keyData: Data(repeating: 0x51, count: RepoEncryptionKeyMaterial.byteCount)
        )
        try keyStore.save(material)
        let manifest = VersionManifestLite.makeEncryptedManifest(
            createdAt: "2026-07-09T00:00:00Z",
            createdBy: "restore-test",
            repoID: material.repoID,
            activeKeyID: material.keyID,
            keyCheck: try RepoEncryptionKeyCodec.keyCheck(
                repoID: material.repoID,
                keyID: material.keyID,
                keyData: material.keyData
            )
        )
        await fresh.seedFile(path: RepoLayoutLite.versionPath(basePath: profile().basePath), data: try VersionManifestLite.encode(manifest))
        let factory = OrderedRestoreClientFactory(initial: initial, fresh: fresh)
        let service = RestoreService(makeClient: { _, _ in factory.makeClient() }, encryptionKeyStore: keyStore)
        let encrypted = RestoreService.RestoreItemDescriptor(
            instances: [
                instance(
                    fileName: "encrypted.wmenc",
                    resourceHash: Data(repeating: 0x22, count: RemoteManifestResource.contentHashByteCount),
                    storageCodec: RemoteManifestResource.encryptedStorageCodec,
                    encryptionKeyID: material.keyID
                )
            ],
            identity: Data([0xE2])
        )

        do {
            _ = try await service.restoreItems(
                items: [encrypted],
                profile: profile(),
                password: "pw",
                onItemCompleted: { _, _, _ in }
            )
            XCTFail("expected resource download to fail after encryption context recovery")
        } catch {
            // expected: context load recovers, then the encrypted resource itself is absent on the fresh client.
        }

        let freshAttempts = await fresh.downloadAttemptPaths
        XCTAssertTrue(
            freshAttempts.contains(RepoLayoutLite.versionPath(basePath: profile().basePath)),
            "encryption context was not retried on the fresh client: \(freshAttempts)"
        )
        XCTAssertTrue(
            freshAttempts.contains { $0.hasSuffix("/2026/06/encrypted.wmenc") },
            "encrypted resource download was not reached after context recovery: \(freshAttempts)"
        )
    }

    private final class OrderedRestoreClientFactory: @unchecked Sendable {
        private let lock = NSLock()
        private var count = 0
        private let initial: InMemoryRemoteStorageClient
        private let fresh: InMemoryRemoteStorageClient

        init(initial: InMemoryRemoteStorageClient, fresh: InMemoryRemoteStorageClient) {
            self.initial = initial
            self.fresh = fresh
        }

        func makeClient() -> InMemoryRemoteStorageClient {
            lock.withLock {
                count += 1
                return count == 1 ? initial : fresh
            }
        }
    }
}
