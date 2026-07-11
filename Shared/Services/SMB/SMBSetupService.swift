import Foundation

#if canImport(AMSMB2)
import AMSMB2
#endif

struct SMBShareInfo: Sendable {
    let name: String
    let comment: String
}

final class SMBSetupService {
    static let operationTimeout: TimeInterval = 30

    func listShares(auth: SMBServerAuthContext) async throws -> [SMBShareInfo] {
        #if canImport(AMSMB2)
        return try await boundedOperation(auth: auth) { manager in
            let shares = try await manager.listShares(enumerateHidden: false)
            return shares.map { SMBShareInfo(name: $0.name, comment: $0.comment) }
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func listDirectories(auth: SMBServerAuthContext, shareName: String, path: String) async throws -> [RemoteStorageEntry] {
        #if canImport(AMSMB2)
        return try await boundedOperation(auth: auth) { manager in
            try await manager.connectShare(name: try SMBPathCanonicalizer.canonicalShareName(shareName))
            try Task.checkCancellation()
            let normalized = try SMBPathCanonicalizer.canonicalRawPath(path)
            let items = try await manager.contentsOfDirectory(atPath: normalized, recursive: false)
            return items.compactMap { values in
                let name = (values[.nameKey] as? String) ?? ""
                if name == "." || name == ".." { return nil }

                let isDirectory = (values[.isDirectoryKey] as? Bool) ?? false
                guard isDirectory else { return nil }
                let creationDate = values[.creationDateKey] as? Date
                let modificationDate = values[.contentModificationDateKey] as? Date
                let size = (values[.fileSizeKey] as? NSNumber)?.int64Value ?? 0

                guard let fullPath = try? SMBPathCanonicalizer.canonicalRawPath("\(normalized)/\(name)") else { return nil }
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
    private func boundedOperation<T: Sendable>(
        auth: SMBServerAuthContext,
        body: @escaping @Sendable (SMB2Manager) async throws -> T
    ) async throws -> T {
        let deadline = Date().addingTimeInterval(Self.operationTimeout)
        let handle = SMBSetupAbandonmentHandle()
        let outcome = await NetworkRecovery.boundedAttempt(
            deadline: deadline,
            onAbandon: { handle.abandon() },
            reap: { (_: Result<T, Error>) in await handle.reap() },
            op: { [self] in
                do {
                    return .success(try await withResolvedManager(
                        auth: auth,
                        deadline: deadline,
                        handle: handle,
                        body
                    ))
                } catch {
                    return .failure(error)
                }
            }
        )
        switch outcome {
        case .completed(.success(let value)):
            return value
        case .completed(.failure(let error)):
            throw error
        case .timedOut:
            if Task.isCancelled { throw CancellationError() }
            throw RemoteStorageClientError.unavailable
        }
    }

    // Runs `body` on a manager built from the resolved IPv4 fast path; on a transport-layer failure (stale /
    // unreachable record) retries once on the original hostname. Cancellation is never turned into the slow path.
    private func withResolvedManager<T>(
        auth: SMBServerAuthContext,
        deadline: Date,
        handle: SMBSetupAbandonmentHandle,
        _ body: @escaping @Sendable (SMB2Manager) async throws -> T
    ) async throws -> T {
        guard let normalizedHost = RemoteHostEndpoint.socketHost(auth.host, strippingSMBScheme: true) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        if let ip = await HostnameResolver.resolvedIPv4(normalizedHost), ip != normalizedHost {
            do {
                return try await runAttempt(
                    manager: try makeManager(host: ip, auth: auth, deadline: deadline),
                    handle: handle,
                    body
                )
            } catch {
                if error is CancellationError || Task.isCancelled { throw error }
            }
        }
        try Task.checkCancellation()
        guard Date() < deadline else { throw RemoteStorageClientError.unavailable }
        return try await runAttempt(
            manager: try makeManager(host: normalizedHost, auth: auth, deadline: deadline),
            handle: handle,
            body
        )
    }

    private func runAttempt<T>(
        manager: SMB2Manager,
        handle: SMBSetupAbandonmentHandle,
        _ body: @escaping @Sendable (SMB2Manager) async throws -> T
    ) async throws -> T {
        guard handle.install(manager) else { throw CancellationError() }
        do {
            let value = try await body(manager)
            try Task.checkCancellation()
            try? await manager.disconnectShare(gracefully: false)
            return value
        } catch {
            if !Task.isCancelled {
                try? await manager.disconnectShare(gracefully: false)
            }
            throw error
        }
    }

    private func makeManager(host: String, auth: SMBServerAuthContext, deadline: Date) throws -> SMB2Manager {
        guard let url = SMBEndpoint.url(host: host, port: auth.port) else {
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
        manager.timeout = min(Self.operationTimeout, max(1, deadline.timeIntervalSinceNow))
        return manager
    }
    #endif
}

#if canImport(AMSMB2)
private final class SMBSetupAbandonmentHandle: @unchecked Sendable {
    private let lock = NSLock()
    private var manager: SMB2Manager?
    private var abandoned = false

    func install(_ manager: SMB2Manager) -> Bool {
        let shouldAbort = lock.withLock {
            self.manager = manager
            return abandoned
        }
        if shouldAbort {
            manager.disconnectShare(gracefully: false, completionHandler: nil)
        }
        return !shouldAbort
    }

    func abandon() {
        let manager = lock.withLock { () -> SMB2Manager? in
            guard !abandoned else { return nil }
            abandoned = true
            return self.manager
        }
        manager?.disconnectShare(gracefully: false, completionHandler: nil)
    }

    func reap() async {
        let manager = lock.withLock { self.manager }
        if let manager {
            try? await manager.disconnectShare(gracefully: false)
        }
    }
}
#endif
