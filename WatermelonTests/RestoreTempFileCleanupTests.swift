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
}
