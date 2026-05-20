import Foundation
import os
@testable import Watermelon

actor InMemoryRemoteStorageClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    // In-memory `move` unconditionally overwrites the destination dictionary entry.
    nonisolated var supportsLivenessSafeOverwriteMove: Bool {
        livenessSafeOverwriteMoveBox.withLock { $0 }
    }
    private nonisolated let livenessSafeOverwriteMoveBox = OSAllocatedUnfairLock(initialState: true)
    nonisolated func setSupportsLivenessSafeOverwriteMove(_ value: Bool) {
        livenessSafeOverwriteMoveBox.withLock { $0 = value }
    }
    enum AtomicCreateMode: Sendable {
        case strictlyAtomic     // POSIX O_EXCL / S3 If-None-Match — returns .created
        case bestEffort         // SMB exists+upload — returns .bestEffortRetry on success
        case alwaysAlreadyExists // Concurrent-writer simulation — never lets create through
    }

    enum InjectedError: Error, Equatable {
        case transport
        case permission
        case notFound
    }

    private(set) var files: [String: Data] = [:]
    private(set) var explicitDirectories: Set<String> = ["/"]
    private(set) var connected = false
    private(set) var disconnectCount = 0

    private var atomicCreateMode: AtomicCreateMode = .strictlyAtomic
    private var bestEffortRaceBytes: [String: Data] = [:]
    private var bestEffortOverwritePaths: Set<String> = []
    private var listErrorByPath: [String: InjectedError] = [:]
    private var metadataErrorByPath: [String: InjectedError] = [:]
    private var downloadErrorByPath: [String: InjectedError] = [:]
    private var persistentDownloadErrorByPath: [String: InjectedError] = [:]
    private var uploadErrorByPath: [String: InjectedError] = [:]
    private var deleteErrorByPath: [String: InjectedError] = [:]
    private var injectedMtimes: [String: Date] = [:]

    init() {}


    func setAtomicCreateMode(_ mode: AtomicCreateMode) {
        atomicCreateMode = mode
    }

    func setAlwaysAlreadyExistsBaselineBytes(_ bytes: Data?) {
        alwaysAlreadyExistsBaselineBytes = bytes
    }
    private var alwaysAlreadyExistsBaselineBytes: Data?

    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        atomicCreateGuaranteeBox.withLock { $0 }
    }
    private nonisolated let atomicCreateGuaranteeBox = OSAllocatedUnfairLock(initialState: CreateGuarantee.overwritePossible)

    nonisolated func setAtomicCreateGuarantee(_ guarantee: CreateGuarantee) {
        atomicCreateGuaranteeBox.withLock { $0 = guarantee }
    }

    func stageBestEffortRace(at path: String, with bytes: Data) {
        bestEffortRaceBytes[Self.normalize(path)] = bytes
    }

    func stageBestEffortOverwriteOfExistingPath(at path: String) {
        bestEffortOverwritePaths.insert(Self.normalize(path))
    }

    func isBestEffortOverwriteStaged(at path: String) -> Bool {
        bestEffortOverwritePaths.contains(Self.normalize(path))
    }

    func injectFile(path: String, data: Data) {
        let key = Self.normalize(path)
        files[key] = data
        ensureDirectoryChain(for: key)
    }

    func setModificationDateForTest(_ date: Date, path: String) {
        let key = Self.normalize(path)
        injectedMtimes[key] = date
    }

    func injectFile(path: String, contents: String) {
        injectFile(path: path, data: Data(contents.utf8))
    }

    func corrupt(path: String, with replacement: Data) {
        let key = Self.normalize(path)
        guard files[key] != nil else { return }
        files[key] = replacement
    }

    func truncateInHalf(path: String) {
        let key = Self.normalize(path)
        guard let bytes = files[key] else { return }
        files[key] = bytes.prefix(bytes.count / 2)
    }

    func injectListError(_ error: InjectedError, for path: String) {
        listErrorByPath[Self.normalize(path)] = error
    }

    func injectListURLErrorCancelled(for path: String) {
        listURLCancelByPath.insert(Self.normalize(path))
    }
    private var listURLCancelByPath: Set<String> = []

    func injectListWrappedURLCancellation(for path: String) {
        listWrappedURLCancelByPath.insert(Self.normalize(path))
    }
    private var listWrappedURLCancelByPath: Set<String> = []

    func injectMetadataError(_ error: InjectedError, for path: String) {
        metadataErrorByPath[Self.normalize(path)] = error
    }

    func injectDownloadError(_ error: InjectedError, for path: String) {
        downloadErrorByPath[Self.normalize(path)] = error
    }

    func injectPersistentDownloadError(_ error: InjectedError, for path: String) {
        persistentDownloadErrorByPath[Self.normalize(path)] = error
    }

    func clearPersistentDownloadError(for path: String) {
        persistentDownloadErrorByPath.removeValue(forKey: Self.normalize(path))
    }

    func injectDownloadCancellation(for path: String) {
        downloadCancelByPath.insert(Self.normalize(path))
    }
    private var downloadCancelByPath: Set<String> = []

    func injectDownloadURLErrorCancelled(for path: String) {
        downloadURLCancelByPath.insert(Self.normalize(path))
    }
    private var downloadURLCancelByPath: Set<String> = []

    func injectDownloadWrappedURLCancellation(for path: String) {
        downloadWrappedURLCancelByPath.insert(Self.normalize(path))
    }
    private var downloadWrappedURLCancelByPath: Set<String> = []

    func injectAtomicCreateURLErrorCancelled(for path: String) {
        atomicCreateURLCancelByPath.insert(Self.normalize(path))
    }
    private var atomicCreateURLCancelByPath: Set<String> = []

    func injectNextDownloadURLErrorCancelled() {
        nextDownloadURLCancelArmed = true
    }
    private var nextDownloadURLCancelArmed = false

    func injectUploadError(_ error: InjectedError, for path: String) {
        uploadErrorByPath[Self.normalize(path)] = error
    }

    func injectDeleteError(_ error: InjectedError, for path: String) {
        deleteErrorByPath[Self.normalize(path)] = error
    }

    func snapshotFiles() -> [String: Data] {
        files
    }

    func hasFile(_ path: String) -> Bool {
        files[Self.normalize(path)] != nil
    }


    func connect() async throws {
        connected = true
    }

    func disconnect() async {
        connected = false
        disconnectCount += 1
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        nil
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let dir = Self.normalize(path)
        if listURLCancelByPath.remove(dir) != nil {
            throw NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        }
        if listWrappedURLCancelByPath.remove(dir) != nil {
            throw RemoteStorageClientError.underlying(
                NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
            )
        }
        if let err = listErrorByPath.removeValue(forKey: dir) {
            throw Self.translate(err)
        }
        guard explicitDirectories.contains(dir) || hasAnyDescendant(of: dir) else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileReadNoSuchFileError,
                userInfo: [NSLocalizedDescriptionKey: "directory not found"]
            ))
        }
        var entries: [RemoteStorageEntry] = []
        var seenDirs: Set<String> = []
        let prefix = dir == "/" ? "/" : dir + "/"
        for (filePath, data) in files {
            guard filePath.hasPrefix(prefix) else { continue }
            let rest = String(filePath.dropFirst(prefix.count))
            guard !rest.isEmpty else { continue }
            if let slashRange = rest.range(of: "/") {
                let dirName = String(rest[..<slashRange.lowerBound])
                guard seenDirs.insert(dirName).inserted else { continue }
                let dirPath = (dir == "/" ? "" : dir) + "/" + dirName
                entries.append(RemoteStorageEntry(
                    path: dirPath,
                    name: dirName,
                    isDirectory: true,
                    size: 0,
                    creationDate: nil,
                    modificationDate: nil
                ))
            } else {
                entries.append(RemoteStorageEntry(
                    path: filePath,
                    name: rest,
                    isDirectory: false,
                    size: Int64(data.count),
                    creationDate: nil,
                    modificationDate: injectedMtimes[filePath]
                ))
            }
        }
        // Also include explicitly-empty subdirectories (e.g. fresh ensureSubdirectories).
        for explicitDir in explicitDirectories where explicitDir.hasPrefix(prefix) {
            let rest = String(explicitDir.dropFirst(prefix.count))
            guard !rest.isEmpty, !rest.contains("/") else { continue }
            guard seenDirs.insert(rest).inserted else { continue }
            entries.append(RemoteStorageEntry(
                path: explicitDir,
                name: rest,
                isDirectory: true,
                size: 0,
                creationDate: nil,
                modificationDate: nil
            ))
        }
        return entries
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let key = Self.normalize(path)
        if let err = metadataErrorByPath.removeValue(forKey: key) {
            throw Self.translate(err)
        }
        if let data = files[key] {
            return RemoteStorageEntry(
                path: key,
                name: (key as NSString).lastPathComponent,
                isDirectory: false,
                size: Int64(data.count),
                creationDate: nil,
                modificationDate: injectedMtimes[key]
            )
        }
        if explicitDirectories.contains(key) || hasAnyDescendant(of: key) {
            return RemoteStorageEntry(
                path: key,
                name: (key as NSString).lastPathComponent,
                isDirectory: true,
                size: 0,
                creationDate: nil,
                modificationDate: nil
            )
        }
        return nil
    }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        let key = Self.normalize(remotePath)
        if let err = uploadErrorByPath.removeValue(forKey: key) {
            throw Self.translate(err)
        }
        if respectTaskCancellation { try Task.checkCancellation() }
        let data = try Data(contentsOf: localURL)
        files[key] = data
        ensureDirectoryChain(for: key)
        onProgress?(1.0)
    }

    func atomicCreate(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        let key = Self.normalize(remotePath)
        if atomicCreateURLCancelByPath.remove(key) != nil {
            throw NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        }
        if let err = uploadErrorByPath.removeValue(forKey: key) {
            throw Self.translate(err)
        }
        if respectTaskCancellation { try Task.checkCancellation() }
        onProgress?(1.0)
        switch atomicCreateMode {
        case .alwaysAlreadyExists:
            if let baseline = alwaysAlreadyExistsBaselineBytes, files[key] == nil {
                files[key] = baseline
                ensureDirectoryChain(for: key)
            }
            return .alreadyExists
        case .strictlyAtomic:
            if files[key] != nil { return .alreadyExists }
            let data = try Data(contentsOf: localURL)
            files[key] = data
            ensureDirectoryChain(for: key)
            return .created
        case .bestEffort:
            // Silent-overwrite race: peer's bytes are present, but our `exists` check
            // raced ahead at T0 and `upload` at T1 replaces them — mirrors AMSMB2's
            // `uploadItem(toPath:)` semantics on a same-key collision.
            if bestEffortOverwritePaths.remove(key) != nil {
                let data = try Data(contentsOf: localURL)
                files[key] = data
                ensureDirectoryChain(for: key)
                return .bestEffortRetry
            }
            if files[key] != nil { return .alreadyExists }
            if let raceBytes = bestEffortRaceBytes.removeValue(forKey: key) {
                files[key] = raceBytes
                ensureDirectoryChain(for: key)
                return .bestEffortRetry
            }
            let data = try Data(contentsOf: localURL)
            files[key] = data
            ensureDirectoryChain(for: key)
            return .bestEffortRetry
        }
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        // No-op — in-memory store has no concept of mtime.
    }

    func download(remotePath: String, localURL: URL) async throws {
        let key = Self.normalize(remotePath)
        if nextDownloadURLCancelArmed {
            nextDownloadURLCancelArmed = false
            throw NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        }
        if downloadCancelByPath.remove(key) != nil {
            throw CancellationError()
        }
        if downloadURLCancelByPath.remove(key) != nil {
            throw NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        }
        if downloadWrappedURLCancelByPath.remove(key) != nil {
            throw RemoteStorageClientError.underlying(
                NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
            )
        }
        if let stuck = persistentDownloadErrorByPath[key] {
            throw Self.translate(stuck)
        }
        if let err = downloadErrorByPath.removeValue(forKey: key) {
            throw Self.translate(err)
        }
        guard let data = files[key] else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileNoSuchFileError,
                userInfo: [NSLocalizedDescriptionKey: "no such file"]
            ))
        }
        try data.write(to: localURL, options: .atomic)
    }

    func exists(path: String) async throws -> Bool {
        let key = Self.normalize(path)
        return files[key] != nil
    }

    func delete(path: String) async throws {
        let key = Self.normalize(path)
        if let err = deleteErrorByPath.removeValue(forKey: key) {
            // Peer-race fidelity: a real `.notFound` from a backend means the path
            // is already gone, so reflect that in the fake before throwing.
            if err == .notFound {
                removeFileOrDirectory(at: key)
            }
            throw Self.translate(err)
        }
        removeFileOrDirectory(at: key)
    }

    private func removeFileOrDirectory(at key: String) {
        if files.removeValue(forKey: key) != nil { return }
        // Directory delete — remove all descendants and the explicit-dir mark.
        let prefix = key == "/" ? "/" : key + "/"
        for child in files.keys where child.hasPrefix(prefix) {
            files.removeValue(forKey: child)
        }
        explicitDirectories = explicitDirectories.filter { $0 != key && !$0.hasPrefix(prefix) }
    }

    func createDirectory(path: String) async throws {
        let key = Self.normalize(path)
        if createDirURLCancelByPath.remove(key) != nil {
            throw NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        }
        explicitDirectories.insert(key)
        ensureDirectoryChain(for: key)
    }

    func injectCreateDirectoryURLErrorCancelled(for path: String) {
        createDirURLCancelByPath.insert(Self.normalize(path))
    }
    private var createDirURLCancelByPath: Set<String> = []

    func move(from sourcePath: String, to destinationPath: String) async throws {
        let src = Self.normalize(sourcePath)
        let dst = Self.normalize(destinationPath)
        if let data = files.removeValue(forKey: src) {
            files[dst] = data
            ensureDirectoryChain(for: dst)
            return
        }
        throw RemoteStorageClientError.underlying(NSError(
            domain: NSCocoaErrorDomain,
            code: NSFileNoSuchFileError,
            userInfo: [NSLocalizedDescriptionKey: "no such file"]
        ))
    }

    nonisolated var moveIfAbsentGuarantee: CreateGuarantee {
        moveIfAbsentGuaranteeBox.withLock { $0 }
    }
    private nonisolated let moveIfAbsentGuaranteeBox = OSAllocatedUnfairLock(initialState: CreateGuarantee.exclusive)

    nonisolated func setMoveIfAbsentGuarantee(_ guarantee: CreateGuarantee) {
        moveIfAbsentGuaranteeBox.withLock { $0 = guarantee }
    }

    nonisolated var readAfterWriteGraceSeconds: TimeInterval {
        readAfterWriteGraceBox.withLock { $0 }
    }
    private nonisolated let readAfterWriteGraceBox = OSAllocatedUnfairLock(initialState: TimeInterval(0))

    nonisolated func setReadAfterWriteGrace(_ seconds: TimeInterval) {
        readAfterWriteGraceBox.withLock { $0 = seconds }
    }

    func supportsExclusiveMoveIfAbsent(forDestinationPath _: String) async throws -> Bool {
        if let probe = exclusiveMoveProbeOverride { return probe }
        return moveIfAbsentGuarantee == .exclusive
    }

    private var exclusiveMoveProbeOverride: Bool?

    func setExclusiveMoveProbeOverride(_ value: Bool?) {
        exclusiveMoveProbeOverride = value
    }

    private var moveIfAbsentOutcomeOverride: AtomicCreateResult?
    private var preMoveSourceMutation: (@Sendable (_ sourcePath: String) async -> Void)?

    func setMoveIfAbsentOutcomeOverride(_ outcome: AtomicCreateResult?) {
        moveIfAbsentOutcomeOverride = outcome
    }

    func setPreMoveSourceMutation(_ mutation: (@Sendable (_ sourcePath: String) async -> Void)?) {
        preMoveSourceMutation = mutation
    }

    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        let src = Self.normalize(sourcePath)
        let dst = Self.normalize(destinationPath)
        if let mutation = preMoveSourceMutation {
            preMoveSourceMutation = nil
            await mutation(src)
        }
        if let override = moveIfAbsentOutcomeOverride {
            moveIfAbsentOutcomeOverride = nil
            switch override {
            case .created:
                if files[dst] != nil { return .alreadyExists }
                guard let data = files.removeValue(forKey: src) else {
                    throw RemoteStorageClientError.underlying(NSError(
                        domain: NSCocoaErrorDomain,
                        code: NSFileNoSuchFileError,
                        userInfo: [NSLocalizedDescriptionKey: "no such file"]
                    ))
                }
                files[dst] = data
                ensureDirectoryChain(for: dst)
                return .created
            case .alreadyExists:
                return .alreadyExists
            case .bestEffortRetry:
                guard let data = files[src] else {
                    throw RemoteStorageClientError.underlying(NSError(
                        domain: NSCocoaErrorDomain,
                        code: NSFileNoSuchFileError,
                        userInfo: [NSLocalizedDescriptionKey: "no such file"]
                    ))
                }
                files[dst] = data
                ensureDirectoryChain(for: dst)
                return .bestEffortRetry
            }
        }
        if files[dst] != nil {
            return .alreadyExists
        }
        guard let data = files.removeValue(forKey: src) else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileNoSuchFileError,
                userInfo: [NSLocalizedDescriptionKey: "no such file"]
            ))
        }
        files[dst] = data
        ensureDirectoryChain(for: dst)
        return .created
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        let src = Self.normalize(sourcePath)
        let dst = Self.normalize(destinationPath)
        guard let data = files[src] else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileNoSuchFileError,
                userInfo: [NSLocalizedDescriptionKey: "no such file"]
            ))
        }
        files[dst] = data
        ensureDirectoryChain(for: dst)
    }


    private func ensureDirectoryChain(for path: String) {
        var components = path.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
        if components.isEmpty { return }
        components.removeLast() // exclude the file/leaf segment
        var prefix = ""
        explicitDirectories.insert("/")
        for segment in components {
            prefix += "/" + segment
            explicitDirectories.insert(prefix)
        }
    }

    private func hasAnyDescendant(of dir: String) -> Bool {
        let prefix = dir == "/" ? "/" : dir + "/"
        return files.keys.contains { $0.hasPrefix(prefix) }
            || explicitDirectories.contains { $0 != dir && $0.hasPrefix(prefix) }
    }

    private static func translate(_ injected: InjectedError) -> Error {
        switch injected {
        case .transport:
            return RemoteStorageClientError.underlying(NSError(
                domain: NSURLErrorDomain,
                code: NSURLErrorNotConnectedToInternet,
                userInfo: [NSLocalizedDescriptionKey: "transport failure"]
            ))
        case .permission:
            return RemoteStorageClientError.underlying(NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileReadNoPermissionError,
                userInfo: [NSLocalizedDescriptionKey: "permission denied"]
            ))
        case .notFound:
            return RemoteStorageClientError.underlying(NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileNoSuchFileError,
                userInfo: [NSLocalizedDescriptionKey: "no such file"]
            ))
        }
    }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        let collapsed = trimmed
            .split(separator: "/", omittingEmptySubsequences: true)
            .joined(separator: "/")
        return "/" + collapsed
    }
}
