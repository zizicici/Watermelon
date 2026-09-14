import Foundation

final class AppCacheManager: @unchecked Sendable {
    enum Category: CaseIterable, Sendable {
        case thumbnails, originals, temporaryFiles, stagedFiles, executionLogs
        case localData, otherData

        var allowsClearing: Bool { self != .localData && self != .otherData }
    }

    struct Usage: Sendable {
        var bytes: Int64 = 0
        var clearableBytes: Int64 = 0
        var clearableFiles = 0
        var hasErrors = false
    }

    struct DataEntry: Sendable {
        enum Kind: Sendable {
            case remoteIndex, caches, applicationSupport, temporary, documents, library, unclassified
            case database, databaseLog, databaseMemory, databaseJournal, preferences, localFiles
        }

        let path: String
        let kind: Kind
        var usage = Usage()
    }

    struct DataDetails: Sendable {
        let total: Usage
        let entries: [DataEntry]
    }

    struct Locations: Sendable {
        let thumbnails: [URL]
        let originals: URL
        let temporary: URL
        let executionLogs: URL
        var bundle: URL? = nil
        var dataRoots: [URL] = []
        var localData: [URL] = []
        var documents: URL? = nil
        var library: URL? = nil
        var database: URL? = nil

        var stagedFiles: URL { temporary.appendingPathComponent("MediaDrop", isDirectory: true) }
    }

    struct Snapshot: Sendable {
        let categories: [Category: Usage]
        let app: Usage
        let data: Usage
    }

    static let shared = AppCacheManager(locations: Locations(
        thumbnails: MediaThumbnailCache.directoryURLs,
        originals: OriginalPhotoCache.shared.directoryURL,
        temporary: FileManager.default.temporaryDirectory,
        executionLogs: ExecutionLogFileStore.rootDirectory,
        bundle: Bundle.main.bundleURL,
        dataRoots: [
            URL.documentsDirectory, URL.libraryDirectory, FileManager.default.temporaryDirectory,
        ],
        localData: [
            DatabaseManager.defaultDatabaseURL().deletingLastPathComponent(),
            URL.libraryDirectory.appendingPathComponent("Preferences", isDirectory: true),
        ],
        documents: URL.documentsDirectory,
        library: URL.libraryDirectory,
        database: DatabaseManager.defaultDatabaseURL()
    ), thumbnailClearer: { await MediaThumbnailCache.clearIncludingLegacyCache() })

    private struct Identity: Hashable {
        let device: UInt64
        let inode: UInt64
        let created: Date
    }

    private struct File: Hashable {
        let url: URL
        let identity: Identity
        let bytes: Int64
        let modified: Date
    }

    private let locations: Locations
    private let access = LocalCacheFileAccess.shared
    private let previousTemporaryFiles: [URL: File]
    private let previousLinkedImportFiles: Set<URL>
    private let previousStagingSessions: [URL: Identity]
    private let thumbnailClearer: (@Sendable () async -> Void)?

    init(locations: Locations, thumbnailClearer: (@Sendable () async -> Void)? = nil) {
        self.locations = locations
        self.thumbnailClearer = thumbnailClearer
        // Capture before any producers start; copied media can retain dates from years ago.
        let temporary = Self.children(of: locations.temporary)
        var linkedImportFiles: Set<URL> = []
        previousTemporaryFiles = Dictionary(uniqueKeysWithValues: temporary.compactMap { url in
            guard Self.isOwnedTemporaryFile(url.lastPathComponent),
                  let file = try? Self.file(at: url) else { return nil }
            if url.lastPathComponent.hasPrefix("imp_"),
               let attributes = try? FileManager.default.attributesOfItem(atPath: url.path),
               let references = attributes[.referenceCount] as? NSNumber, references.intValue > 1 {
                linkedImportFiles.insert(url)
            }
            return (url, file)
        })
        previousLinkedImportFiles = linkedImportFiles
        previousStagingSessions = Dictionary(uniqueKeysWithValues: Self.children(of: locations.stagedFiles).compactMap { url in
            guard UUID(uuidString: url.lastPathComponent) != nil,
                  let attributes = try? FileManager.default.attributesOfItem(atPath: url.path),
                  attributes[.type] as? FileAttributeType == .typeDirectory,
                  let identity = Self.identity(attributes) else { return nil }
            return (url, identity)
        })
    }

    func usage() -> [Category: Usage] {
        Dictionary(uniqueKeysWithValues: Category.allCases.map { ($0, scan($0).usage) })
    }

    func usage(for category: Category) -> Usage {
        scan(category).usage
    }

    func otherDataDetails() -> DataDetails {
        dataDetails(for: .otherData, describe: otherDataEntry)
    }

    func localDataDetails() -> DataDetails {
        dataDetails(for: .localData, describe: localDataEntry)
    }

    private func dataDetails(for category: Category, describe: (URL, Bool) -> DataEntry) -> DataDetails {
        let result = scan(category)
        var entries: [String: DataEntry] = [:]
        var counted: [String: Set<Identity>] = [:]
        for file in result.files {
            let entry = describe(file.url, false)
            if counted[entry.path, default: []].insert(file.identity).inserted {
                entries[entry.path, default: entry].usage.bytes += file.bytes
            }
        }
        for url in result.failedURLs {
            let entry = describe(url, true)
            entries[entry.path, default: entry].usage.hasErrors = true
        }
        return DataDetails(total: result.usage, entries: entries.values.sorted { $0.path < $1.path })
    }

    private func localDataEntry(for url: URL, isDirectory: Bool) -> DataEntry {
        let path = url.standardizedFileURL.path
        var kind: DataEntry.Kind = .localFiles
        if !isDirectory, let database = locations.database?.standardizedFileURL.path {
            switch path {
            case database: kind = .database
            case database + "-wal": kind = .databaseLog
            case database + "-shm": kind = .databaseMemory
            case database + "-journal": kind = .databaseJournal
            default: break
            }
        }
        if let library = locations.library {
            let rootPath = library.standardizedFileURL.path
            if path.hasPrefix(rootPath + "/") {
                let relative = "Library" + path.dropFirst(rootPath.count)
                if relative == "Library/Preferences" || relative.hasPrefix("Library/Preferences/") { kind = .preferences }
                return DataEntry(path: relative, kind: kind)
            }
        }
        for root in locations.localData {
            let rootPath = root.standardizedFileURL.path
            if path == rootPath || path.hasPrefix(rootPath + "/") {
                return DataEntry(path: root.lastPathComponent + path.dropFirst(rootPath.count), kind: kind)
            }
        }
        return DataEntry(path: url.lastPathComponent, kind: kind)
    }

    private func otherDataEntry(for url: URL, isDirectory: Bool) -> DataEntry {
        var roots: [(URL, String, DataEntry.Kind)] = []
        if let library = locations.library {
            roots += [
                (library.appendingPathComponent("Application Support/RemoteManifestSnapshotCache"),
                 "Library/Application Support/RemoteManifestSnapshotCache", .remoteIndex),
                (library.appendingPathComponent("Caches"), "Library/Caches", .caches),
                (library.appendingPathComponent("Application Support"), "Library/Application Support", .applicationSupport),
                (library, "Library", .library),
            ]
        }
        roots.append((locations.temporary, "tmp", .temporary))
        if let documents = locations.documents { roots.append((documents, "Documents", .documents)) }
        roots += locations.dataRoots.map { ($0, $0.lastPathComponent, .unclassified) }
        let path = url.standardizedFileURL.path
        for (root, label, kind) in roots {
            let rootPath = root.standardizedFileURL.path
            guard path == rootPath || path.hasPrefix(rootPath + "/") else { continue }
            let relative = path.dropFirst(rootPath.count).split(separator: "/")
            if kind != .remoteIndex, let first = relative.first, isDirectory || relative.count > 1 {
                return DataEntry(path: label + "/" + first, kind: kind)
            }
            return DataEntry(path: label, kind: kind)
        }
        return DataEntry(path: url.lastPathComponent, kind: .unclassified)
    }

    func snapshot() -> Snapshot {
        let scans = Category.allCases.map { ($0, scan($0)) }
        var data = Usage()
        var counted: Set<Identity> = []
        for (_, result) in scans {
            data.hasErrors = data.hasErrors || result.usage.hasErrors
            for file in result.files where counted.insert(file.identity).inserted {
                data.bytes += file.bytes
            }
        }
        var app = Usage()
        if let bundle = locations.bundle {
            var files: [File] = []
            collect(bundle, category: .localData, files: &files, hasErrors: &app.hasErrors)
            var counted: Set<Identity> = []
            for file in files where counted.insert(file.identity).inserted { app.bytes += file.bytes }
        }
        return Snapshot(categories: Dictionary(uniqueKeysWithValues: scans.map { ($0.0, $0.1.usage) }), app: app, data: data)
    }

    func clear(_ category: Category) async -> Bool {
        guard category.allowsClearing else { return false }
        if category == .thumbnails, let thumbnailClearer {
            await thumbnailClearer()
            return await Task.detached(priority: .utility) {
                let result = self.scan(category)
                return !result.usage.hasErrors && result.files.isEmpty
            }.value
        }
        return await Task.detached(priority: .utility) { self.clearFiles(category) }.value
    }

    @discardableResult
    func clearFiles(_ category: Category) -> Bool {
        guard category.allowsClearing else { return false }
        let result = scan(category)
        var success = !result.usage.hasErrors
        for file in result.files {
            access.withAccess {
                guard canClear(file, category: category) else { return }
                do {
                    guard try Self.file(at: file.url) == file else { return }
                    try FileManager.default.removeItem(at: file.url)
                } catch {
                    if !Self.isMissing(error) { success = false }
                }
            }
        }
        return success
    }

    private func scan(_ category: Category) -> (usage: Usage, files: [File], failedURLs: [URL]) {
        var files: [File] = []
        var usage = Usage()
        var failedURLs: [URL] = []
        for root in roots(for: category) {
            collect(root, category: category, files: &files, hasErrors: &usage.hasErrors) { failedURLs.append($0) }
        }
        var counted: Set<Identity> = []
        var clearable: Set<Identity> = []
        for file in files {
            if counted.insert(file.identity).inserted { usage.bytes += file.bytes }
            if canClear(file, category: category) {
                usage.clearableFiles += 1
                if clearable.insert(file.identity).inserted { usage.clearableBytes += file.bytes }
            }
        }
        return (usage, files, failedURLs)
    }

    private func roots(for category: Category) -> [URL] {
        switch category {
        case .thumbnails: return locations.thumbnails
        case .originals: return [locations.originals]
        case .temporaryFiles: return [locations.temporary]
        case .stagedFiles: return [locations.stagedFiles]
        case .executionLogs: return [locations.executionLogs]
        case .localData: return locations.localData
        case .otherData: return locations.dataRoots
        }
    }

    private func collect(
        _ directory: URL, category: Category, files: inout [File], hasErrors: inout Bool,
        onError: ((URL) -> Void)? = nil
    ) {
        guard !Task.isCancelled else { return }
        if category == .otherData && isClassifiedDirectory(directory) { return }
        do {
            let attributes = try FileManager.default.attributesOfItem(atPath: directory.path)
            guard attributes[.type] as? FileAttributeType == .typeDirectory else {
                hasErrors = true
                onError?(directory)
                return
            }
            let children = try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil)
            for url in children {
                guard !Task.isCancelled else { return }
                do {
                    let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
                    if attributes[.type] as? FileAttributeType == .typeDirectory {
                        if category != .temporaryFiles {
                            collect(url, category: category, files: &files, hasErrors: &hasErrors, onError: onError)
                        }
                    } else if let file = try Self.file(at: url), accepts(file, category: category) {
                        files.append(file)
                    }
                } catch {
                    if !Self.isMissing(error) {
                        hasErrors = true
                        onError?(url.deletingLastPathComponent())
                    }
                }
            }
        } catch {
            if !Self.isMissing(error) {
                hasErrors = true
                onError?(directory)
            }
        }
    }

    private func accepts(_ file: File, category: Category) -> Bool {
        switch category {
        case .temporaryFiles: return Self.isOwnedTemporaryFile(file.url.lastPathComponent)
        case .otherData:
            if file.url.deletingLastPathComponent().standardizedFileURL.path == locations.temporary.standardizedFileURL.path {
                return !Self.isOwnedTemporaryFile(file.url.lastPathComponent)
            }
            return true
        default: return true
        }
    }

    private func canClear(_ file: File, category: Category) -> Bool {
        guard category.allowsClearing, !access.isProtected(file.url) else { return false }
        switch category {
        case .executionLogs:
            return file.url.pathExtension == "log"
        case .temporaryFiles:
            guard let previous = previousTemporaryFiles[file.url] else { return false }
            if previous == file { return true }
            // A cache LRU touch also changes old import aliases' modification dates.
            return previousLinkedImportFiles.contains(file.url)
                && previous.identity == file.identity && previous.bytes == file.bytes
        case .stagedFiles:
            let session = file.url.deletingLastPathComponent()
            guard let expected = previousStagingSessions[session],
                  let attributes = try? FileManager.default.attributesOfItem(atPath: session.path) else { return false }
            return attributes[.type] as? FileAttributeType == .typeDirectory && Self.identity(attributes) == expected
        default:
            return true
        }
    }

    private func isClassifiedDirectory(_ url: URL) -> Bool {
        let path = url.standardizedFileURL.path
        return Category.allCases.filter { $0 != .temporaryFiles && $0 != .otherData }.contains { category in
            roots(for: category).contains { root in
                let rootPath = root.standardizedFileURL.path
                return path == rootPath || path.hasPrefix(rootPath + "/")
            }
        }
    }

    private static func file(at url: URL) throws -> File? {
        let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
        guard attributes[.type] as? FileAttributeType == .typeRegular else { return nil }
        guard let identity = identity(attributes),
              let size = attributes[.size] as? NSNumber,
              let modified = attributes[.modificationDate] as? Date else { throw CocoaError(.fileReadUnknown) }
        return File(url: url, identity: identity, bytes: size.int64Value, modified: modified)
    }

    private static func identity(_ attributes: [FileAttributeKey: Any]) -> Identity? {
        guard let device = attributes[.systemNumber] as? NSNumber,
              let inode = attributes[.systemFileNumber] as? NSNumber,
              let created = attributes[.creationDate] as? Date else { return nil }
        return Identity(device: device.uint64Value, inode: inode.uint64Value, created: created)
    }

    private static func children(of url: URL) -> [URL] {
        (try? FileManager.default.contentsOfDirectory(at: url, includingPropertiesForKeys: nil)) ?? []
    }

    private static func isMissing(_ error: Error) -> Bool {
        let error = error as NSError
        return error.domain == NSCocoaErrorDomain && (error.code == NSFileNoSuchFileError || error.code == NSFileReadNoSuchFileError)
    }

    static func isOwnedTemporaryFile(_ name: String) -> Bool {
        ownedTemporaryPattern.firstMatch(in: name, range: NSRange(name.startIndex..., in: name)) != nil
    }

    private static let ownedTemporaryPattern: NSRegularExpression = {
        let uuid = "[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
        let prefixes = [
            "restore_", "restore_import_", "orig_", "imp_", "live_local_", "thumb_dl_",
            "Watermelon-Transfer-", ".sftp-download-", "sftp-copy-", "smb-copy-",
            "leftover-hash-", "leftover-unique-", "leftover-resource-catalog-",
            "manifest-validate-", "orphan-validate-", "orphan-version-", "legacy-v1-prune-marker-",
            "v1-prune-version-", "v1-prune-", "v1lite_", "legacy-v1-prune-", "browser-link-artifact-",
        ].map(NSRegularExpression.escapedPattern(for:)).joined(separator: "|")
        let patterns = [
            "(?:\(prefixes))?\(uuid)(?:\\..*)?",
            "(?:thumb_|thumb_up_)[0-9a-fA-F]+_\(uuid)\\.jpg",
            "remote_compare_\(uuid)_.+",
            "month_manifest_[0-9]+_[0-9]+_\(uuid)\\.sqlite(?:-(?:wal|shm|journal))?",
            "movecheck-\(uuid)(?:-verify)?",
            "s3-probe-\(uuid)-(?:a|b|download)",
            "\\.watermelon-probe-\(uuid)-(?:upload-a|upload-b|download)",
        ]
        let pattern = "^(?:" + patterns.joined(separator: "|") + ")$"
        return try! NSRegularExpression(pattern: pattern)
    }()
}
