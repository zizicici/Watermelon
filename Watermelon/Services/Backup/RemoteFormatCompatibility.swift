import Foundation

enum WatermelonRemoteFormat {
    static let markerDirectoryName = ".watermelon"
    static let versionFileName = "version.json"
}

struct WatermelonRemoteVersionManifest: Decodable {
    let formatVersion: Int?
    let minAppVersion: String?
    let createdAt: String?
    let createdBy: String?

    enum CodingKeys: String, CodingKey {
        case formatVersion = "format_version"
        case minAppVersion = "min_app_version"
        case createdAt = "created_at"
        case createdBy = "created_by"
    }
}

enum BackupCompatibilityError: LocalizedError {
    case remoteFormatUnsupported(minAppVersion: String?)

    var errorDescription: String? {
        switch self {
        case .remoteFormatUnsupported(let minAppVersion):
            if let minAppVersion {
                return String.localizedStringWithFormat(
                    String(localized: "compatibility.error.remoteFormatUnsupported.versioned"),
                    AppName.localized,
                    minAppVersion
                )
            }
            return String.localizedStringWithFormat(
                String(localized: "compatibility.error.remoteFormatUnsupported"),
                AppName.localized
            )
        }
    }
}

struct RemoteFormatCompatibilityService: Sendable {
    func verify(client: any RemoteStorageClientProtocol, profile: ServerProfileRecord) async throws {
        let basePath = RemotePathBuilder.normalizePath(profile.basePath)
        let entries = try await client.list(path: basePath)
        let markerExists = entries.contains { entry in
            entry.isDirectory && entry.name == WatermelonRemoteFormat.markerDirectoryName
        }
        guard markerExists else { return }

        let detected = await readMinAppVersion(client: client, profile: profile)
        throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: detected)
    }

    private func readMinAppVersion(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord
    ) async -> String? {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("watermelon-version-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let absolutePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: "\(WatermelonRemoteFormat.markerDirectoryName)/\(WatermelonRemoteFormat.versionFileName)"
        )

        do {
            try await client.download(remotePath: absolutePath, localURL: tempURL)
            let data = try Data(contentsOf: tempURL)
            let manifest = try JSONDecoder().decode(WatermelonRemoteVersionManifest.self, from: data)
            if let version = manifest.minAppVersion?.trimmingCharacters(in: .whitespacesAndNewlines),
               !version.isEmpty {
                return version
            }
            return nil
        } catch {
            return nil
        }
    }
}
