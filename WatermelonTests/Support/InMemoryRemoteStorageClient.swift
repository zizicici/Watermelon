import Foundation
import os
@testable import Watermelon

/// Fake `RemoteStorageClientProtocol` for V2 backup tests.
///
/// Stores files as `[normalizedPath: Data]` in an actor; directories are implicit
/// (any path with at least one child). Configurable atomicCreate semantics so a single
/// fixture can simulate POSIX (`.created`), SMB exists+upload (`.bestEffortRetry`), or
/// concurrent-writer collision (`.alreadyExists`).
///
/// Also exposes `injectFile` / `corrupt` / `setListError` test hooks so we can stage
/// crash-recovery / corruption / transport-failure scenarios without touching real I/O.
actor InMemoryRemoteStorageClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    // In-memory `move` unconditionally overwrites the destination dictionary entry.
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
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
    /// One-shot injection for `.bestEffort` mode — when set, the next atomicCreate at
    /// `path` stores the injected bytes (NOT the uploaded ones) and returns
    /// `.bestEffortRetry`, simulating a peer who raced ahead on a non-atomic backend.
    private var bestEffortRaceBytes: [String: Data] = [:]
    /// Per-path operation injection — when set, the next matching call throws this error
    /// once. Cleared after firing so tests can chain "fail once, then succeed".
    private var listErrorByPath: [String: InjectedError] = [:]
    private var metadataErrorByPath: [String: InjectedError] = [:]
    private var downloadErrorByPath: [String: InjectedError] = [:]
    private var persistentDownloadErrorByPath: [String: InjectedError] = [:]
    private var uploadErrorByPath: [String: InjectedError] = [:]
    private var injectedMtimes: [String: Date] = [:]

    init() {}

    // MARK: - Test hooks

    func setAtomicCreateMode(_ mode: AtomicCreateMode) {
        atomicCreateMode = mode
    }

    /// Default mimics `.overwritePossible` so writers exercise the gate's
    /// staging-fallback path. `setAtomicCreateMode` only affects atomicCreate's
    /// return shape, not the reported guarantee.
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        atomicCreateGuaranteeBox.withLock { $0 }
    }
    private nonisolated let atomicCreateGuaranteeBox = OSAllocatedUnfairLock(initialState: CreateGuarantee.overwritePossible)

    /// Lets tests flip to `.exclusive` so the gate exercises its direct-atomic path
    /// (skips staging, skips post-verify on `.created`).
    nonisolated func setAtomicCreateGuarantee(_ guarantee: CreateGuarantee) {
        atomicCreateGuaranteeBox.withLock { $0 = guarantee }
    }

    /// Stage a one-shot bestEffort race: the next `atomicCreate(remotePath:)` will
    /// return `.bestEffortRetry` after writing `bytes` to the file (instead of the
    /// caller's uploaded bytes). Use to exercise the verify-on-remote retry path.
    func stageBestEffortRace(at path: String, with bytes: Data) {
        bestEffortRaceBytes[Self.normalize(path)] = bytes
    }

    /// Pre-populate a file at `path` with `data`. Use this to stage half-bootstrapped
    /// repos, V1 manifests, foreign-repo snapshots, etc. before running the code under test.
    func injectFile(path: String, data: Data) {
        let key = Self.normalize(path)
        files[key] = data
        ensureDirectoryChain(for: key)
    }

    /// Real backends always have an mtime; tests want deterministic age.
    func setModificationDateForTest(_ date: Date, path: String) {
        let key = Self.normalize(path)
        injectedMtimes[key] = date
    }

    func injectFile(path: String, contents: String) {
        injectFile(path: path, data: Data(contents.utf8))
    }

    /// Truncate or replace bytes at `path`. Used to simulate `integrityMismatch` /
    /// `decodeFailure` paths in the snapshot/commit readers.
    func corrupt(path: String, with replacement: Data) {
        let key = Self.normalize(path)
        guard files[key] != nil else { return }
        files[key] = replacement
    }

    /// Truncate file to half its size — produces a SHA mismatch on next read.
    func truncateInHalf(path: String) {
        let key = Self.normalize(path)
        guard let bytes = files[key] else { return }
        files[key] = bytes.prefix(bytes.count / 2)
    }

    func injectListError(_ error: InjectedError, for path: String) {
        listErrorByPath[Self.normalize(path)] = error
    }

    func injectMetadataError(_ error: InjectedError, for path: String) {
        metadataErrorByPath[Self.normalize(path)] = error
    }

    func injectDownloadError(_ error: InjectedError, for path: String) {
        downloadErrorByPath[Self.normalize(path)] = error
    }

    /// Persists across attempts — every `download` for this path throws until
    /// `clearPersistentDownloadError` is called. Use for testing retry-loop exhaustion.
    func injectPersistentDownloadError(_ error: InjectedError, for path: String) {
        persistentDownloadErrorByPath[Self.normalize(path)] = error
    }

    func clearPersistentDownloadError(for path: String) {
        persistentDownloadErrorByPath.removeValue(forKey: Self.normalize(path))
    }

    func injectUploadError(_ error: InjectedError, for path: String) {
        uploadErrorByPath[Self.normalize(path)] = error
    }

    func snapshotFiles() -> [String: Data] {
        files
    }

    /// Returns true if a file (not directory) exists at the given path.
    func hasFile(_ path: String) -> Bool {
        files[Self.normalize(path)] != nil
    }

    // MARK: - RemoteStorageClientProtocol

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
                modificationDate: nil
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
        if let err = uploadErrorByPath.removeValue(forKey: key) {
            throw Self.translate(err)
        }
        if respectTaskCancellation { try Task.checkCancellation() }
        onProgress?(1.0)
        switch atomicCreateMode {
        case .alwaysAlreadyExists:
            return .alreadyExists
        case .strictlyAtomic:
            if files[key] != nil { return .alreadyExists }
            let data = try Data(contentsOf: localURL)
            files[key] = data
            ensureDirectoryChain(for: key)
            return .created
        case .bestEffort:
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
        explicitDirectories.insert(key)
        ensureDirectoryChain(for: key)
    }

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
    /// Default mirrors the protocol's fail-closed `.overwritePossible` so the gate exercises probe + finalization branches.
    /// Tests modeling LocalVolume / S3-conditional opt into `.exclusive` via `setMoveIfAbsentGuarantee`.
    private nonisolated let moveIfAbsentGuaranteeBox = OSAllocatedUnfairLock(initialState: CreateGuarantee.overwritePossible)

    /// Lets per-test scenarios swap `.exclusive` ↔ `.overwritePossible` to exercise the gate's two finalization paths.
    nonisolated func setMoveIfAbsentGuarantee(_ guarantee: CreateGuarantee) {
        moveIfAbsentGuaranteeBox.withLock { $0 = guarantee }
    }

    nonisolated var readAfterWriteGraceSeconds: TimeInterval {
        readAfterWriteGraceBox.withLock { $0 }
    }
    private nonisolated let readAfterWriteGraceBox = OSAllocatedUnfairLock(initialState: TimeInterval(0))

    /// Lets per-test scenarios simulate eventually-consistent backends (R2/MinIO/WebDAV-behind-cache).
    nonisolated func setReadAfterWriteGrace(_ seconds: TimeInterval) {
        readAfterWriteGraceBox.withLock { $0 = seconds }
    }

    func supportsExclusiveMoveIfAbsent(forDestinationPath _: String) async throws -> Bool {
        if let probe = exclusiveMoveProbeOverride { return probe }
        return moveIfAbsentGuarantee == .exclusive
    }

    private var exclusiveMoveProbeOverride: Bool?

    /// Overrides the runtime probe so the gate can be tested with "probe says yes/no" independently of the static guarantee.
    func setExclusiveMoveProbeOverride(_ value: Bool?) {
        exclusiveMoveProbeOverride = value
    }

    private var moveIfAbsentOutcomeOverride: AtomicCreateResult?
    private var preMoveSourceMutation: (@Sendable (_ sourcePath: String) async -> Void)?

    /// One-shot: next `moveIfAbsent` returns this outcome with production-like remote state.
    /// `.bestEffortRetry` copies source bytes to destination and leaves the source visible so the
    /// caller's post-write verifier can equality-check then delete. Cleared on use.
    func setMoveIfAbsentOutcomeOverride(_ outcome: AtomicCreateResult?) {
        moveIfAbsentOutcomeOverride = outcome
    }

    /// One-shot: invoked before the next `moveIfAbsent` materializes its decision, then cleared.
    /// Lets a test mutate (e.g. delete) the source between the caller's pre-scan and the move.
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

    // MARK: - Internals

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

    /// Normalize to leading-slash, no trailing slash, no `//`. Empty / "." → "/".
    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        let collapsed = trimmed
            .split(separator: "/", omittingEmptySubsequences: true)
            .joined(separator: "/")
        return "/" + collapsed
    }
}
