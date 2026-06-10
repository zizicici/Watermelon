import Foundation

enum WatermelonRemoteFormat {
    static let markerDirectoryName = ".watermelon"
    static let versionFileName = "version.json"
}

struct WatermelonRemoteVersionManifest: Codable, Equatable {
    let formatVersion: Int?
    let layout: String?
    let minAppVersion: String?
    let createdAt: String?
    let createdBy: String?

    enum CodingKeys: String, CodingKey {
        case formatVersion = "format_version"
        case layout
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
    // Flag-off V1 compatibility gate. A future flag-off build rejects only a *parseable committed*
    // `.watermelon/version.json` (a real Lite/foreign format commit) and tolerates a half-created marker —
    // a bare `.watermelon`, an empty `.watermelon/locks`, or an unparsable version.json — so it can still
    // operate the V1 tree beneath. Transport/list/download faults are surfaced (fail closed), never read as
    // safe V1. NOTE: already-released clients reject ANY `.watermelon`; this relaxation only protects future
    // flag-off builds (it cannot change a binary already in the field).
    func verify(client: any RemoteStorageClientProtocol, profile: ServerProfileRecord) async throws {
        let basePath = RemotePathBuilder.normalizePath(profile.basePath)
        let entries = try await client.list(path: basePath)
        let markerExists = entries.contains { entry in
            entry.isDirectory && entry.name == WatermelonRemoteFormat.markerDirectoryName
        }
        guard markerExists else { return }

        switch await probeCommittedVersion(client: client, basePath: profile.basePath) {
        case .committed(let minAppVersion):
            throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
        case .absentOrUnparsable:
            return   // half-created/unparsable marker with no committed version: tolerate, stay V1
        case .fault(let error):
            throw error   // a probe fault must surface, never silently proceed as safe V1
        }
    }

    private enum VersionProbe {
        case committed(minAppVersion: String?)   // parseable version.json with a format version: a real commit
        case absentOrUnparsable                  // no version.json, or present but undecodable/incomplete
        case fault(Error)                        // non-notFound download fault: cannot prove committed-or-not
    }

    private func probeCommittedVersion(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async -> VersionProbe {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("watermelon-version-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)
        do {
            try await client.download(remotePath: versionPath, localURL: tempURL)
        } catch {
            if RemoteFaultLite.classify(error) == .notFound { return .absentOrUnparsable }
            return .fault(error)
        }

        // A committed version requires the format marker to decode with a format version; anything else is
        // a half-written/unparsable marker, not a committed Lite/foreign repo.
        guard let data = try? Data(contentsOf: tempURL),
              let manifest = try? VersionManifestLite.decode(data),
              manifest.formatVersion != nil else {
            return .absentOrUnparsable
        }
        let minAppVersion = manifest.minAppVersion?.trimmingCharacters(in: .whitespacesAndNewlines)
        return .committed(minAppVersion: (minAppVersion?.isEmpty == false) ? minAppVersion : nil)
    }
}
