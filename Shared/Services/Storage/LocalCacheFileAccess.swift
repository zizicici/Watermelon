import Foundation

final class LocalCacheFileAccess: @unchecked Sendable {
    static let shared = LocalCacheFileAccess()

    final class Lease: @unchecked Sendable {
        private let owner: LocalCacheFileAccess
        private let token: UUID

        fileprivate init(owner: LocalCacheFileAccess, token: UUID) {
            self.owner = owner
            self.token = token
        }

        deinit {
            owner.withAccess { owner.paths.removeValue(forKey: token) }
        }
    }

    private let lock = NSRecursiveLock()
    private var paths: [UUID: String] = [:]

    func protect(_ url: URL) -> Lease {
        withAccess {
            let token = UUID()
            paths[token] = url.standardizedFileURL.path
            return Lease(owner: self, token: token)
        }
    }

    func isProtected(_ url: URL) -> Bool {
        let path = url.standardizedFileURL.path
        return withAccess {
            paths.values.contains { protected in
                path == protected || path.hasPrefix(protected + "/") || protected.hasPrefix(path + "/")
            }
        }
    }

    @discardableResult
    func withAccess<T>(_ body: () throws -> T) rethrows -> T {
        lock.lock()
        defer { lock.unlock() }
        return try body()
    }
}
