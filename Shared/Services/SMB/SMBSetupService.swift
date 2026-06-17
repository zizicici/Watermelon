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
        let shares = try await withResolvedManager(auth: auth) { manager in
            try await manager.listShares(enumerateHidden: false)
        }
        return shares.map { SMBShareInfo(name: $0.name, comment: $0.comment) }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func listDirectories(auth: SMBServerAuthContext, shareName: String, path: String) async throws -> [RemoteStorageEntry] {
        #if canImport(AMSMB2)
        return try await withResolvedManager(auth: auth) { manager in
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
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    #if canImport(AMSMB2)
    // Runs `body` on a manager built from the resolved IPv4 fast path; on a transport-layer failure (stale /
    // unreachable record) retries once on the original hostname. Cancellation is never turned into the slow path.
    private func withResolvedManager<T>(
        auth: SMBServerAuthContext,
        _ body: (SMB2Manager) async throws -> T
    ) async throws -> T {
        let normalizedHost = auth.host.replacingOccurrences(of: "smb://", with: "")
        if let ip = await HostnameResolver.resolvedIPv4(normalizedHost), ip != normalizedHost {
            do {
                return try await body(try makeManager(host: ip, auth: auth))
            } catch {
                if error is CancellationError || Task.isCancelled { throw error }
                // IPv4 fast path failed; retry the original hostname below.
            }
        }
        return try await body(try makeManager(host: normalizedHost, auth: auth))
    }

    private func makeManager(host: String, auth: SMBServerAuthContext) throws -> SMB2Manager {
        guard let url = URL(string: "smb://\(host):\(auth.port)") else {
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
    }
    #endif
}
