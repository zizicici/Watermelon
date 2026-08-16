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
    private var pathsByID: [IDKey: Set<String>] = [:]
    // Directories whose children were fully paged, so an uncached child is known absent rather than unseen.
    private var enumeratedDirectories: Set<Key> = []

    func noteEnumerated(namespace: Namespace, directory: String) {
        lock.withLock { _ = enumeratedDirectories.insert(Key(namespace: namespace, path: directory)) }
    }

    // True while the directory's enumeration still describes it: nothing has been written since the listing,
    // so both "this name is there" and "this name is not" are answerable without a round trip.
    func describesCurrentChildren(namespace: Namespace, directory: String) -> Bool {
        lock.withLock { enumeratedDirectories.contains(Key(namespace: namespace, path: directory)) }
    }

    // Coarse on purpose: a directory enumeration is only trustworthy while nothing has been written, and the
    // win is in the read-only startup phase. Correctness here beats keeping it alive across writes.
    func dropEnumerations(namespace: Namespace) {
        lock.withLock { enumeratedDirectories = enumeratedDirectories.filter { $0.namespace != namespace } }
    }

    func item(namespace: Namespace, path: String) -> OneDriveDriveItem? {
        lock.withLock { itemsByPath[Key(namespace: namespace, path: path)] }
    }

    func cache(_ item: OneDriveDriveItem, namespace: Namespace, path: String) {
        lock.withLock {
            let pathKey = Key(namespace: namespace, path: path)
            if let previous = itemsByPath[pathKey], previous.id != item.id {
                let previousIDKey = IDKey(namespace: namespace, itemID: previous.id)
                pathsByID[previousIDKey]?.remove(path)
                if pathsByID[previousIDKey]?.isEmpty == true {
                    pathsByID.removeValue(forKey: previousIDKey)
                }
            }
            itemsByPath[pathKey] = item
            let idKey = IDKey(namespace: namespace, itemID: item.id)
            pathsByID[idKey, default: []].insert(path)
        }
    }

    func remove(namespace: Namespace, id: String) {
        lock.withLock {
            let idKey = IDKey(namespace: namespace, itemID: id)
            for path in pathsByID[idKey] ?? [] {
                itemsByPath.removeValue(forKey: Key(namespace: namespace, path: path))
            }
            pathsByID.removeValue(forKey: idKey)
        }
    }
}

actor OneDriveThrottleGate {
    struct Key: Hashable, Sendable {
        let authorityEnvironment: String
        let homeAccountIdentifier: String
    }

    private var blockedUntilByKey: [Key: Date] = [:]

    func requirePermit(for key: Key, now: Date = Date()) throws {
        guard let blockedUntil = blockedUntilByKey[key] else { return }
        guard blockedUntil > now else {
            blockedUntilByKey.removeValue(forKey: key)
            return
        }
        throw OneDriveErrorClassifier.makeServiceError(
            statusCode: 429,
            code: "throttledRequest",
            message: String(localized: "onedrive.error.graph.throttled")
        )
    }

    func record(retryAfter: Date, for key: Key) {
        if let blockedUntil = blockedUntilByKey[key], blockedUntil >= retryAfter { return }
        blockedUntilByKey[key] = retryAfter
    }

    func waitForPermit(for key: Key) async throws {
        while let deadline = blockedUntilByKey[key] {
            let delay = deadline.timeIntervalSinceNow
            guard delay > 0 else {
                blockedUntilByKey.removeValue(forKey: key)
                return
            }
            try await Task.sleep(for: .seconds(delay))
        }
    }
}

nonisolated final class OneDriveSharedState: Sendable {
    let throttleGate = OneDriveThrottleGate()
    let itemIndex = OneDriveItemIndex()
}
