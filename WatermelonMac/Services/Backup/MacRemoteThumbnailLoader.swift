import Foundation

enum MacRemoteThumbnailFetchPolicy {
    static func shouldFetch(
        hasLocalAsset: Bool,
        hasFingerprint: Bool,
        hasCachedData: Bool,
        sessionIsCurrent: Bool
    ) -> Bool {
        !hasLocalAsset
            && hasFingerprint
            && !hasCachedData
            && sessionIsCurrent
    }
}

actor MacRemoteThumbnailRequestGate {
    struct Permit: Hashable, Sendable {
        fileprivate let id: UUID
    }

    private let limit: Int
    private var active = Set<Permit>()
    private var waitingOrder: [Permit] = []
    private var waiters: [Permit: CheckedContinuation<Bool, Never>] = [:]
    private var cancelledBeforeArrival = Set<Permit>()
    private var isShutdown = false

    init(limit: Int) {
        self.limit = max(1, limit)
    }

    var pendingCount: Int {
        waiters.count
    }

    func wait() async -> Permit? {
        let permit = Permit(id: UUID())
        let granted = await withTaskCancellationHandler {
            await acquire(permit)
        } onCancel: {
            Task {
                await self.cancel(permit)
            }
        }
        guard granted else { return nil }
        guard !Task.isCancelled else {
            release(permit)
            return nil
        }
        return permit
    }

    func release(_ permit: Permit) {
        guard active.remove(permit) != nil else { return }
        grantNext()
    }

    func shutdown() {
        isShutdown = true
        let pending = Array(waiters.values)
        waiters.removeAll()
        waitingOrder.removeAll()
        cancelledBeforeArrival.removeAll()
        for continuation in pending {
            continuation.resume(returning: false)
        }
    }

    private func acquire(_ permit: Permit) async -> Bool {
        guard !isShutdown else { return false }
        if cancelledBeforeArrival.remove(permit) != nil {
            return false
        }
        if active.count < limit {
            active.insert(permit)
            return true
        }
        return await withCheckedContinuation { continuation in
            if isShutdown
                || cancelledBeforeArrival.remove(permit) != nil {
                continuation.resume(returning: false)
                return
            }
            waitingOrder.append(permit)
            waiters[permit] = continuation
        }
    }

    private func cancel(_ permit: Permit) {
        guard !isShutdown, !active.contains(permit) else { return }
        if let continuation = waiters.removeValue(forKey: permit) {
            waitingOrder.removeAll { $0 == permit }
            continuation.resume(returning: false)
        } else {
            cancelledBeforeArrival.insert(permit)
        }
    }

    private func grantNext() {
        guard !isShutdown else { return }
        while !waitingOrder.isEmpty {
            let permit = waitingOrder.removeFirst()
            guard let continuation = waiters.removeValue(
                forKey: permit
            ) else {
                continue
            }
            active.insert(permit)
            continuation.resume(returning: true)
            return
        }
    }
}

final class MacRemoteThumbnailLoader: @unchecked Sendable {
    private let profile: ServerProfileRecord
    private let pool: StorageClientPool
    private let requestGate = MacRemoteThumbnailRequestGate(limit: 2)

    init(
        profile: ServerProfileRecord,
        credential: String,
        storageClientFactory: StorageClientFactory
    ) {
        self.profile = profile
        self.pool = StorageClientPool(maxConnections: 2) {
            try storageClientFactory.makeClient(
                profile: profile,
                credentialPayload: credential
            )
        }
    }

    func data(for fingerprint: Data) async -> Data? {
        guard !Task.isCancelled,
              let permit = await requestGate.wait() else {
            return nil
        }
        guard !Task.isCancelled else {
            await requestGate.release(permit)
            return nil
        }
        let data = await loadData(for: fingerprint)
        await requestGate.release(permit)
        return data
    }

    private func loadData(for fingerprint: Data) async -> Data? {
        let client: any RemoteStorageClientProtocol
        do {
            client = try await pool.acquire()
        } catch {
            return nil
        }
        guard !Task.isCancelled else {
            await pool.release(client, reusable: false)
            return nil
        }
        let path = RemoteThumbnailPaths.absolutePath(
            basePath: profile.basePath,
            fingerprintHex: fingerprint.hexString
        )
        do {
            let data: Data
            if let directURL = await client.directReadURL(
                forRemotePath: path
            ) {
                data = try Data(contentsOf: directURL)
            } else {
                let url = FileManager.default.temporaryDirectory
                    .appendingPathComponent(
                        "wm-thumb-\(UUID().uuidString).jpg"
                    )
                defer { try? FileManager.default.removeItem(at: url) }
                try await client.download(
                    remotePath: path,
                    localURL: url
                )
                data = try Data(contentsOf: url)
            }
            guard !Task.isCancelled else {
                await pool.release(client, reusable: false)
                return nil
            }
            await pool.release(client, reusable: true)
            return data
        } catch {
            let reusable = !Task.isCancelled
                && RemoteFaultLite.classify(error) != .cancelled
                && !profile.isConnectionUnavailableError(error)
            await pool.release(client, reusable: reusable)
            return nil
        }
    }

    func disconnect() async {
        await requestGate.shutdown()
        await pool.shutdown()
    }
}
