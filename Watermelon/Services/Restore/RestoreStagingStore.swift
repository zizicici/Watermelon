import Foundation

final class RestoreStagingStore: Sendable {
    static let rootDirectory = FileManager.default.temporaryDirectory.appendingPathComponent("Restore", isDirectory: true)
    let directory: URL

    init(rootDirectory: URL = RestoreStagingStore.rootDirectory) throws {
        directory = rootDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    }

    deinit { removeAll() }

    func itemDirectory(_ index: Int) throws -> URL {
        let url = directory.appendingPathComponent(String(index), isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    func removeAll() {
        try? FileManager.default.removeItem(at: directory)
    }

    static func availableCapacity() -> Int64? {
        let values = try? FileManager.default.temporaryDirectory.resourceValues(forKeys: [
            .volumeAvailableCapacityForImportantUsageKey, .volumeAvailableCapacityKey,
        ])
        return values?.volumeAvailableCapacityForImportantUsage ?? values?.volumeAvailableCapacity.map(Int64.init)
    }

    @discardableResult
    static func cleanupStaleSessions(in rootDirectory: URL = RestoreStagingStore.rootDirectory) -> Task<Void, Never> {
        let stale = (try? FileManager.default.contentsOfDirectory(at: rootDirectory, includingPropertiesForKeys: nil)) ?? []
        return Task.detached(priority: .utility) {
            for directory in stale { try? FileManager.default.removeItem(at: directory) }
        }
    }
}
