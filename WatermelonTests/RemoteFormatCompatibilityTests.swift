import Foundation
import XCTest
@testable import Watermelon

final class RemoteFormatCompatibilityTests: XCTestCase {
    func testVerifyPassesWhenMarkerDirectoryIsMissing() async throws {
        let client = MockCompatibilityClient(entriesByPath: [
            "/backup": [
                RemoteStorageEntry(
                    path: "/backup/2026",
                    name: "2026",
                    isDirectory: true,
                    size: 0,
                    creationDate: nil,
                    modificationDate: nil
                )
            ]
        ])

        try await RemoteFormatCompatibilityService().verify(
            client: client,
            profile: makeProfile(basePath: "/backup")
        )
    }

    func testVerifyThrowsWhenMarkerDirectoryExists() async throws {
        let client = MockCompatibilityClient(
            entriesByPath: [
                "/backup": [
                    RemoteStorageEntry(
                        path: "/backup/.watermelon",
                        name: ".watermelon",
                        isDirectory: true,
                        size: 0,
                        creationDate: nil,
                        modificationDate: nil
                    )
                ]
            ],
            downloadsByPath: [
                "/backup/.watermelon/version.json": Data(#"{"min_app_version":"2.0"}"#.utf8)
            ]
        )

        do {
            try await RemoteFormatCompatibilityService().verify(
                client: client,
                profile: makeProfile(basePath: "/backup")
            )
            XCTFail("Expected remote format poison marker to reject the profile")
        } catch let error as BackupCompatibilityError {
            guard case .remoteFormatUnsupported(let minAppVersion) = error else {
                return XCTFail("Unexpected compatibility error: \(error)")
            }
            XCTAssertEqual(minAppVersion, "2.0")
        }
    }

    private func makeProfile(basePath: String) -> ServerProfileRecord {
        ServerProfileRecord(
            id: 1,
            name: "Test",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "host",
            port: 445,
            shareName: "share",
            basePath: basePath,
            username: "user",
            domain: nil,
            credentialRef: "credential",
            createdAt: Date(),
            updatedAt: Date()
        )
    }
}

private final class MockCompatibilityClient: RemoteStorageClientProtocol, @unchecked Sendable {
    private let entriesByPath: [String: [RemoteStorageEntry]]
    private let downloadsByPath: [String: Data]

    init(
        entriesByPath: [String: [RemoteStorageEntry]],
        downloadsByPath: [String: Data] = [:]
    ) {
        self.entriesByPath = entriesByPath
        self.downloadsByPath = downloadsByPath
    }

    func connect() async throws {}

    func disconnect() async {}

    func verifyWriteAccess() async throws {}

    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        entriesByPath[path] ?? []
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? { nil }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {}

    func setModificationDate(_ date: Date, forPath path: String) async throws {}

    func download(remotePath: String, localURL: URL) async throws {
        guard let data = downloadsByPath[remotePath] else {
            throw RemoteStorageClientError.unavailable
        }
        try data.write(to: localURL)
    }

    func exists(path: String) async throws -> Bool { false }

    func delete(path: String) async throws {}

    func createDirectory(path: String) async throws {}

    func move(from sourcePath: String, to destinationPath: String) async throws {}

    func copy(from sourcePath: String, to destinationPath: String) async throws {}
}
