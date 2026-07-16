import Foundation

nonisolated final class OneDriveItemIndex: @unchecked Sendable {
    struct Namespace: Hashable, Sendable {
        let cloudEnvironment: String
        let driveID: String
        let rootItemID: String
    }

    private struct Key: Hashable {
        let namespace: Namespace
        let path: String
    }

    private struct IDKey: Hashable {
        let namespace: Namespace
        let itemID: String
    }

    private let lock = NSLock()
    private var itemsByPath: [Key: OneDriveDriveItem] = [:]
    private var itemsByID: [IDKey: OneDriveDriveItem] = [:]
    private var pathsByID: [IDKey: Set<String>] = [:]

    func item(namespace: Namespace, path: String) -> OneDriveDriveItem? {
        lock.withLock { itemsByPath[Key(namespace: namespace, path: path)] }
    }

    func item(namespace: Namespace, id: String) -> OneDriveDriveItem? {
        lock.withLock { itemsByID[IDKey(namespace: namespace, itemID: id)] }
    }

    func cache(_ item: OneDriveDriveItem, namespace: Namespace, path: String) {
        lock.withLock {
            let pathKey = Key(namespace: namespace, path: path)
            if let previous = itemsByPath[pathKey], previous.id != item.id {
                let previousIDKey = IDKey(namespace: namespace, itemID: previous.id)
                pathsByID[previousIDKey]?.remove(path)
                if pathsByID[previousIDKey]?.isEmpty == true {
                    pathsByID.removeValue(forKey: previousIDKey)
                    itemsByID.removeValue(forKey: previousIDKey)
                }
            }
            itemsByPath[pathKey] = item
            let idKey = IDKey(namespace: namespace, itemID: item.id)
            itemsByID[idKey] = item
            pathsByID[idKey, default: []].insert(path)
        }
    }

    func remove(namespace: Namespace, path: String) {
        lock.withLock {
            let pathKey = Key(namespace: namespace, path: path)
            guard let item = itemsByPath.removeValue(forKey: pathKey) else { return }
            let idKey = IDKey(namespace: namespace, itemID: item.id)
            pathsByID[idKey]?.remove(path)
            if pathsByID[idKey]?.isEmpty == true {
                pathsByID.removeValue(forKey: idKey)
                itemsByID.removeValue(forKey: idKey)
            }
        }
    }

    func remove(namespace: Namespace, id: String) {
        lock.withLock {
            let idKey = IDKey(namespace: namespace, itemID: id)
            for path in pathsByID[idKey] ?? [] {
                itemsByPath.removeValue(forKey: Key(namespace: namespace, path: path))
            }
            pathsByID.removeValue(forKey: idKey)
            itemsByID.removeValue(forKey: idKey)
        }
    }

    func reset(namespace: Namespace) {
        lock.withLock {
            itemsByPath = itemsByPath.filter { $0.key.namespace != namespace }
            itemsByID = itemsByID.filter { $0.key.namespace != namespace }
            pathsByID = pathsByID.filter { $0.key.namespace != namespace }
        }
    }
}

actor OneDriveThrottleGate {
    private var blockedUntil: Date?

    func requirePermit(now: Date = Date()) throws {
        guard let blockedUntil else { return }
        guard blockedUntil > now else {
            self.blockedUntil = nil
            return
        }
        throw OneDriveErrorClassifier.makeServiceError(
            statusCode: 429,
            code: "throttledRequest",
            message: String(localized: "onedrive.error.graph.throttled"),
            retryAfter: blockedUntil,
            claims: nil
        )
    }

    func record(retryAfter: Date) {
        if let blockedUntil, blockedUntil >= retryAfter { return }
        blockedUntil = retryAfter
    }

    func waitForPermit() async throws {
        while let deadline = blockedUntil {
            let delay = deadline.timeIntervalSinceNow
            guard delay > 0 else {
                blockedUntil = nil
                return
            }
            try await Task.sleep(for: .seconds(delay))
        }
    }
}

nonisolated final class OneDriveSharedState: @unchecked Sendable {
    let throttleGate = OneDriveThrottleGate()
    let itemIndex = OneDriveItemIndex()
}
