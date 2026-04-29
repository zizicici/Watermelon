import Foundation

#if canImport(AMSMB2)
import AMSMB2
#endif

struct SMBShareInfo {
    let name: String
    let comment: String
}

final class SMBSetupService {
    func listShares(auth: SMBServerAuthContext) async throws -> [SMBShareInfo] {
        #if canImport(AMSMB2)
        let manager = try makeManager(auth: auth)
        let shares = try await manager.listShares(enumerateHidden: false)
        return shares.map { SMBShareInfo(name: $0.name, comment: $0.comment) }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func listDirectories(auth: SMBServerAuthContext, shareName: String, path: String) async throws -> [RemoteStorageEntry] {
        #if canImport(AMSMB2)
        let manager = try makeManager(auth: auth)
        try await manager.connectShare(name: shareName)
        defer {
            Task {
                try? await manager.disconnectShare(gracefully: false)
            }
        }

        let normalized = RemotePathBuilder.normalizePath(path)
        let items = try await manager.contentsOfDirectory(atPath: normalized, recursive: false)
        return items.compactMap { values in
            let name = (values[.nameKey] as? String) ?? ""
            if name == "." || name == ".." { return nil }

            let isDirectory = (values[.isDirectoryKey] as? Bool) ?? false
            guard isDirectory else { return nil }
            let creationDate = values[.creationDateKey] as? Date
            let modificationDate = values[.contentModificationDateKey] as? Date
            let size = (values[.fileSizeKey] as? NSNumber)?.int64Value ?? 0

            let fullPath = RemotePathBuilder.normalizePath("\(normalized)/\(name)")
            return RemoteStorageEntry(
                path: fullPath,
                name: name,
                isDirectory: true,
                size: size,
                creationDate: creationDate,
                modificationDate: modificationDate
            )
        }.sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    private func makeManager(auth: SMBServerAuthContext) throws -> SMB2Manager {
        #if canImport(AMSMB2)
        let normalizedHost = auth.host.replacingOccurrences(of: "smb://", with: "")
        guard let url = URL(string: "smb://\(normalizedHost):\(auth.port)") else {
            throw RemoteStorageClientError.invalidConfiguration
        }

        let user = [auth.domain, auth.username]
            .compactMap { value -> String? in
                guard let value, !value.isEmpty else { return nil }
                return value
            }
            .joined(separator: ";")

        let credential = URLCredential(
            user: user.isEmpty ? auth.username : user,
            password: auth.password,
            persistence: .forSession
        )

        guard let manager = SMB2Manager(url: url, credential: credential) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return manager
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }
}
