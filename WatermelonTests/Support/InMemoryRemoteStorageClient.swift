import Foundation
@testable import Watermelon

// Errors whose RemoteFaultLite classification is asserted elsewhere; reused to script transport faults.
enum RemoteErrorFixtures {
    static var notFound: Error { NSError(domain: NSPOSIXErrorDomain, code: Int(ENOENT)) }
    static var retryable: Error { NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut) }
    static var terminal: Error { NSError(domain: "WriteLockTestTerminal", code: 1) }
    static var cancelled: Error { CancellationError() }

    // A transient SMB share/redirector outage. Pre-P05 these tokens classified as `.notFound`; they must
    // now be `.retryable` so a blinking share is never read as a missing data directory.
    static func smb(_ token: String) -> Error {
        NSError(domain: "AMSMB2", code: 1, userInfo: [NSLocalizedDescriptionKey: token])
    }
    static var smbBadNetworkName: Error { smb("STATUS_BAD_NETWORK_NAME") }
    static var smbRedirectorNotStarted: Error { smb("STATUS_REDIRECTOR_NOT_STARTED") }
}

// Counts best-effort marker invocations so a test can assert the diagnostic hook fired (or didn't).
actor MarkerRecorder {
    private(set) var count = 0
    func record() { count += 1 }
}

func makeLockEntry(basePath: String, writerID: String, modificationDate: Date?) -> RemoteStorageEntry {
    RemoteStorageEntry(
        path: RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!,
        name: RepoLayoutLite.lockFilename(writerID: writerID)!,
        isDirectory: false,
        size: 0,
        creationDate: nil,
        modificationDate: modificationDate
    )
}

// Actor-isolated fake remote. Backs a tiny in-memory tree so LIST/upload/delete compose naturally,
// and adds FIFO scripts so a test can force exact LIST snapshots (eventual consistency) or transport
// errors. Uploads/deletes/created directories are recorded for inspection.
actor InMemoryRemoteStorageClient: RemoteStorageClientProtocol {
    private enum DownloadScriptStep {
        case data(Data)
        case missingLocalFile
        case failure(Error)
    }

    private struct Node {
        var isDirectory: Bool
        var size: Int64
        var modificationDate: Date?
    }

    private var nodes: [String: Node] = [:]
    private var directories: Set<String> = []
    private var fileContents: [String: Data] = [:]
    // Paths sharing one physical blob after a non-independent MOVE (123pan-style): deleting any member destroys
    // the shared blob, so all members become unreadable. Empty on independent backends.
    private var blobAliases: [String: Set<String>] = [:]

    private var listScript: [Result<[RemoteStorageEntry], Error>] = []
    private var metadataFailureSuffixes: [(suffix: String, error: Error)] = []
    private var uploadFailureSuffixes: [(suffix: String, error: Error)] = []
    // Post-effect: the upload writes the file server-side, THEN throws — models a PUT that lands but whose
    // response the client sees as a failure.
    private var uploadPostEffectFailureFromSuffixes: [(suffix: String, error: Error)] = []
    // Corrupt post-effect: the upload writes GIVEN (e.g. partial/garbage) bytes server-side, THEN throws — models a
    // PUT that lands damaged bytes over the canonical and reports failure.
    private var uploadCorruptThenFailSuffixes: [(suffix: String, bytes: Data, error: Error)] = []
    private var uploadErrorScript: [Error] = []
    private var deleteErrorScript: [Error] = []
    private var createDirectoryErrorScript: [Error] = []
    private var downloadScript: [DownloadScriptStep] = []
    private var moveErrorScript: [Error] = []
    private var movePostErrorScript: [Error] = []
    private var movePostEffectFailureFromSuffixes: [(suffix: String, error: Error)] = []
    private var existsErrorScript: [Error] = []
    private var existsFailureSuffixes: [(suffix: String, error: Error)] = []
    private var existsPostActions: [(suffix: String, action: @Sendable () async -> Void)] = []

    private var pendingUploadModificationDate: Date?

    private var onUpload: (@Sendable () async -> Void)?
    private var onUploadAfterWrite: (@Sendable () async -> Void)?
    private var onMove: (@Sendable (String, String) async -> Void)?
    // Fires after a download has served its bytes (so the current read sees old state); a test can mutate
    // the lock here to make a later confirmation read observe a changed token or freshened mtime.
    private var onDownload: (@Sendable (String) async -> Void)?

    // Connection-aware mode: model real backends (WebDAV/SFTP) that reject delete once disconnected,
    // so a test can prove the foreground lease is released *before* the client is disconnected.
    private var isConnected = true
    private var rejectDeleteAfterDisconnect = false

    // When enabled, request-shaped operations throw CancellationError when Task.isCancelled — matching
    // URLSession-backed backends (WebDAV, S3) that abort in-flight requests in cancelled tasks.
    private var respectTaskCancellation = false

    // When enabled, `move` throws if the destination already exists — modelling no-overwrite rename backends
    // (SFTP v3, SMB) where a move onto an occupied path fails instead of replacing it.
    private var rejectMoveOntoExistingDestination = false
    private var rejectUploadOntoExistingDestination = false
    private var ignoreCreateIfAbsent = false
    private var ignoreSetModificationDate = false

    // When enabled, `move` runs as copy(src→dst) then delete(src) — modelling S3-style backends whose move is
    // a server-side copy plus a separate delete, so a scripted delete fault leaves a published dst with src kept.
    private var moveAsCopyDelete = false
    // Models a server that rejects dot-prefixed FILE names (a common AV/extension filter) while still allowing the
    // `.watermelon` dot-directory. Set via `rejectDotPrefixedFiles()`.
    private var rejectDotPrefixedFileUploads = false

    // Backend capability (immutable, set at construction): whether MOVE may not be independent (123pan-style).
    nonisolated let moveMayNotBeIndependentValue: Bool

    init(moveMayNotBeIndependent: Bool = false) {
        self.moveMayNotBeIndependentValue = moveMayNotBeIndependent
    }

    func rejectDotPrefixedFiles() { rejectDotPrefixedFileUploads = true }

    private(set) var listedPaths: [String] = []
    private(set) var uploadedPaths: [String] = []
    private(set) var deletedPaths: [String] = []
    private(set) var createdDirectories: [String] = []
    private(set) var metadataAttemptPaths: [String] = []
    private(set) var movedPaths: [(from: String, to: String)] = []
    private(set) var copiedPaths: [(from: String, to: String)] = []
    // Every download is recorded (including scripted / not-found attempts) so a test can count how often a
    // path is probed — e.g. to prove a classify is not repeated.
    private(set) var downloadAttemptPaths: [String] = []

    // MARK: - Test configuration

    func seedDirectory(_ path: String) {
        directories.insert(normalize(path))
    }

    func seedFile(path: String, data: Data = Data(), modificationDate: Date? = nil) {
        let key = normalize(path)
        fileContents[key] = data
        nodes[key] = Node(isDirectory: false, size: Int64(data.count), modificationDate: modificationDate)
    }

    // Seeds a lock with a decodable body so confirmation reads (download + decode) see a real token.
    // Pass `body` to forge a specific writer/session/token (e.g. a same-writer successor).
    func seedLock(basePath: String, writerID: String, modificationDate: Date?, body: LockFileBody? = nil) {
        directories.insert(normalize(RepoLayoutLite.locksDirectoryPath(basePath: basePath)))
        let path = normalize(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!)
        let resolvedBody = body ?? LockFileBody(
            writerID: writerID,
            sessionToken: UUID().uuidString,
            lockToken: UUID().uuidString,
            generation: 1
        )
        let data = (try? LockFileCodec.encode(resolvedBody)) ?? Data()
        fileContents[path] = data
        nodes[path] = Node(isDirectory: false, size: Int64(data.count), modificationDate: modificationDate)
    }

    func removeLock(basePath: String, writerID: String) {
        let path = RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!
        nodes[normalize(path)] = nil
    }

    // Seeds a lock whose content does not decode to a LockFileBody (empty/partial/legacy/garbage), so a
    // confirmation read sees no token proof.
    func seedUndecodableLock(basePath: String, writerID: String, modificationDate: Date?) {
        directories.insert(normalize(RepoLayoutLite.locksDirectoryPath(basePath: basePath)))
        let path = normalize(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!)
        let data = Data("not-a-lock-body".utf8)
        fileContents[path] = data
        nodes[path] = Node(isDirectory: false, size: Int64(data.count), modificationDate: modificationDate)
    }

    func setLockModificationDate(basePath: String, writerID: String, to date: Date?) {
        let path = normalize(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!)
        guard var node = nodes[path] else { return }
        node.modificationDate = date
        nodes[path] = node
    }

    func setPendingUploadModificationDate(_ date: Date?) {
        pendingUploadModificationDate = date
    }

    func setOnUpload(_ hook: (@Sendable () async -> Void)?) {
        onUpload = hook
    }

    func setOnUploadAfterWrite(_ hook: (@Sendable () async -> Void)?) {
        onUploadAfterWrite = hook
    }

    func setOnMove(_ hook: (@Sendable (String, String) async -> Void)?) {
        onMove = hook
    }

    func setOnDownload(_ hook: (@Sendable (String) async -> Void)?) {
        onDownload = hook
    }

    func setRejectDeleteAfterDisconnect(_ value: Bool) {
        rejectDeleteAfterDisconnect = value
    }

    func setRespectTaskCancellation(_ value: Bool) {
        respectTaskCancellation = value
    }

    func setRejectMoveOntoExistingDestination(_ value: Bool) {
        rejectMoveOntoExistingDestination = value
    }

    func setRejectUploadOntoExistingDestination(_ value: Bool) {
        rejectUploadOntoExistingDestination = value
    }

    func setIgnoreCreateIfAbsent(_ value: Bool) {
        ignoreCreateIfAbsent = value
    }

    func setIgnoreSetModificationDate(_ value: Bool) {
        ignoreSetModificationDate = value
    }

    func setMoveAsCopyDelete(_ value: Bool) {
        moveAsCopyDelete = value
    }

    var connected: Bool { isConnected }

    func enqueueListResult(_ entries: [RemoteStorageEntry]) {
        listScript.append(.success(entries))
    }

    func enqueueListError(_ error: Error) {
        listScript.append(.failure(error))
    }

    // One-shot: the next `metadata` call whose normalized path ends with `suffix` throws `error`.
    func failMetadata(forPathSuffix suffix: String, error: Error) {
        metadataFailureSuffixes.append((suffix, error))
    }

    // One-shot: the next `exists` call whose normalized path ends with `suffix` throws `error`.
    func failExists(forPathSuffix suffix: String, error: Error) {
        existsFailureSuffixes.append((suffix, error))
    }

    // One-shot: the next `upload` call whose normalized path ends with `suffix` throws `error`.
    func failUploadAfterWrite(forPathSuffix suffix: String, error: Error) {
        uploadPostEffectFailureFromSuffixes.append((suffix, error))
    }

    func failUpload(forPathSuffix suffix: String, error: Error) {
        uploadFailureSuffixes.append((suffix, error))
    }

    // One-shot: the next `upload` to a matching path writes `bytes` (not the local file) then throws `error`.
    func failUploadWritingCorruptBytes(_ bytes: Data, forPathSuffix suffix: String, error: Error) {
        uploadCorruptThenFailSuffixes.append((suffix, bytes, error))
    }

    func enqueueUploadError(_ error: Error) {
        uploadErrorScript.append(error)
    }

    func enqueueDeleteError(_ error: Error) {
        deleteErrorScript.append(error)
    }

    func enqueueCreateDirectoryError(_ error: Error) {
        createDirectoryErrorScript.append(error)
    }

    func enqueueDownloadData(_ data: Data) {
        downloadScript.append(.data(data))
    }

    func enqueueDownloadWithoutLocalFile() {
        downloadScript.append(.missingLocalFile)
    }

    func enqueueDownloadError(_ error: Error) {
        downloadScript.append(.failure(error))
    }

    func enqueueMoveError(_ error: Error) {
        moveErrorScript.append(error)
    }

    func enqueueMovePostError(_ error: Error) {
        movePostErrorScript.append(error)
    }

    // One-shot: a move whose normalized source path ends with `suffix` applies its effect (dst written, src
    // removed) and then throws `error`, modelling a backend that lands the rename before faulting to the caller.
    func failMovePostEffect(fromPathSuffix suffix: String, error: Error) {
        movePostEffectFailureFromSuffixes.append((suffix, error))
    }

    func enqueueExistsError(_ error: Error) {
        existsErrorScript.append(error)
    }

    func enqueueExistsPostAction(forPathSuffix suffix: String, action: @escaping @Sendable () async -> Void) {
        existsPostActions.append((suffix, action))
    }

    // MARK: - Test inspection

    func lockModificationDate(basePath: String, writerID: String) -> Date? {
        let path = normalize(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!)
        return nodes[path]?.modificationDate
    }

    func lockExists(basePath: String, writerID: String) -> Bool {
        let path = normalize(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!)
        return nodes[path] != nil
    }

    func fileData(path: String) -> Data? {
        fileContents[normalize(path)]
    }

    // MARK: - RemoteStorageClientProtocol

    nonisolated func shouldSetModificationDate() -> Bool { true }
    nonisolated func shouldLimitUploadRetries(for _: Error) -> Bool { false }
    // Returns the configured verdict directly (no probe ops) so publishing-path assertions on uploaded/moved
    // stay clean. The real probe is exercised against this mock's alias model in a dedicated test.
    func resolveMoveIsNonIndependent(basePath _: String) async -> Bool { moveMayNotBeIndependentValue }

    func connect() async throws { isConnected = true }
    func disconnect() async { isConnected = false }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        listedPaths.append(path)
        if !listScript.isEmpty {
            switch listScript.removeFirst() {
            case .success(let entries):
                return entries
            case .failure(let error):
                throw error
            }
        }
        let directory = normalize(path)
        guard directoryExists(directory) else { throw RemoteErrorFixtures.notFound }
        let prefix = directory == "/" ? "/" : directory + "/"

        // Synthesize immediate children: file nodes whose remainder has no further slash, plus child
        // directories implied either by a deeper node prefix or an explicitly-seeded directory.
        var fileChildren: [String: Node] = [:]
        var directoryChildNames: Set<String> = []

        for (key, node) in nodes where key.hasPrefix(prefix) {
            let remainder = String(key.dropFirst(prefix.count))
            guard !remainder.isEmpty else { continue }
            if let slash = remainder.firstIndex(of: "/") {
                directoryChildNames.insert(String(remainder[..<slash]))
            } else if node.isDirectory {
                directoryChildNames.insert(remainder)
            } else {
                fileChildren[remainder] = node
            }
        }
        for dir in directories where dir.hasPrefix(prefix) {
            let remainder = String(dir.dropFirst(prefix.count))
            guard !remainder.isEmpty else { continue }
            let name = remainder.firstIndex(of: "/").map { String(remainder[..<$0]) } ?? remainder
            directoryChildNames.insert(name)
        }

        var entries: [RemoteStorageEntry] = []
        for (name, node) in fileChildren where !directoryChildNames.contains(name) {
            entries.append(RemoteStorageEntry(
                path: prefix + name,
                name: name,
                isDirectory: false,
                size: node.size,
                creationDate: nil,
                modificationDate: node.modificationDate
            ))
        }
        for name in directoryChildNames {
            entries.append(RemoteStorageEntry(
                path: prefix + name,
                name: name,
                isDirectory: true,
                size: 0,
                creationDate: nil,
                modificationDate: nil
            ))
        }
        return entries
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let key = normalize(path)
        metadataAttemptPaths.append(path)
        if let index = metadataFailureSuffixes.firstIndex(where: { key.hasSuffix($0.suffix) }) {
            throw metadataFailureSuffixes.remove(at: index).error
        }
        if directories.contains(key) {
            return RemoteStorageEntry(
                path: key,
                name: lastComponent(key),
                isDirectory: true,
                size: 0,
                creationDate: nil,
                modificationDate: nil
            )
        }
        guard let node = nodes[key] else { return nil }
        return RemoteStorageEntry(
            path: key,
            name: lastComponent(key),
            isDirectory: node.isDirectory,
            size: node.size,
            creationDate: nil,
            modificationDate: node.modificationDate
        )
    }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation requestRespectTaskCancellation: Bool,
        onProgress _: ((Double) -> Void)?
    ) async throws {
        try await upload(
            localURL: localURL,
            remotePath: remotePath,
            mode: .replace,
            respectTaskCancellation: requestRespectTaskCancellation,
            onProgress: nil
        )
    }

    func upload(
        localURL: URL,
        remotePath: String,
        mode: RemoteUploadMode,
        respectTaskCancellation requestRespectTaskCancellation: Bool,
        onProgress _: ((Double) -> Void)?
    ) async throws {
        if (respectTaskCancellation || requestRespectTaskCancellation), Task.isCancelled {
            throw CancellationError()
        }
        if let hook = onUpload {
            onUpload = nil
            await hook()
        }
        let key = normalize(remotePath)
        if rejectDotPrefixedFileUploads, (key.split(separator: "/").last).map(String.init)?.hasPrefix(".") == true {
            throw RemoteErrorFixtures.terminal
        }
        if let index = uploadFailureSuffixes.firstIndex(where: { key.hasSuffix($0.suffix) }) {
            throw uploadFailureSuffixes.remove(at: index).error
        }
        if !uploadErrorScript.isEmpty { throw uploadErrorScript.removeFirst() }
        if mode == .createIfAbsent, !ignoreCreateIfAbsent, nodes[key] != nil {
            throw remoteStorageNameCollisionError(path: remotePath)
        }
        if rejectUploadOntoExistingDestination, nodes[key] != nil {
            throw NSError(
                domain: NSPOSIXErrorDomain,
                code: Int(EEXIST),
                userInfo: [NSLocalizedDescriptionKey: "File exists"]
            )
        }
        if let index = uploadCorruptThenFailSuffixes.firstIndex(where: { key.hasSuffix($0.suffix) }) {
            let corrupt = uploadCorruptThenFailSuffixes.remove(at: index)
            uploadedPaths.append(remotePath)
            fileContents[key] = corrupt.bytes
            nodes[key] = Node(isDirectory: false, size: Int64(corrupt.bytes.count), modificationDate: pendingUploadModificationDate)
            breakAlias(key)
            throw corrupt.error
        }
        uploadedPaths.append(remotePath)
        let data = (try? Data(contentsOf: localURL)) ?? Data()
        fileContents[key] = data
        nodes[key] = Node(isDirectory: false, size: Int64(data.count), modificationDate: pendingUploadModificationDate)
        breakAlias(key)   // a fresh PUT writes an independent blob, breaking any prior MOVE alias
        if let hook = onUploadAfterWrite {
            onUploadAfterWrite = nil
            await hook()
        }
        if let index = uploadPostEffectFailureFromSuffixes.firstIndex(where: { key.hasSuffix($0.suffix) }) {
            throw uploadPostEffectFailureFromSuffixes.remove(at: index).error
        }
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        guard !ignoreSetModificationDate else { return }
        let key = normalize(path)
        guard var node = nodes[key] else { return }
        node.modificationDate = date
        nodes[key] = node
    }

    func download(remotePath: String, localURL: URL) async throws {
        if respectTaskCancellation, Task.isCancelled { throw CancellationError() }
        downloadAttemptPaths.append(remotePath)
        if !downloadScript.isEmpty {
            switch downloadScript.removeFirst() {
            case .data(let data):
                try data.write(to: localURL)
                return
            case .missingLocalFile:
                return
            case .failure(let error):
                throw error
            }
        }
        guard let data = fileContents[normalize(remotePath)] else {
            throw RemoteErrorFixtures.notFound
        }
        try data.write(to: localURL)
        if let hook = onDownload {
            await hook(normalize(remotePath))
        }
    }

    func exists(path: String) async throws -> Bool {
        if !existsErrorScript.isEmpty { throw existsErrorScript.removeFirst() }
        if respectTaskCancellation, Task.isCancelled { throw CancellationError() }
        let key = normalize(path)
        if let index = existsFailureSuffixes.firstIndex(where: { key.hasSuffix($0.suffix) }) {
            throw existsFailureSuffixes.remove(at: index).error
        }
        let result = nodes[key] != nil || directories.contains(key)
        if let index = existsPostActions.firstIndex(where: { key.hasSuffix($0.suffix) }) {
            let action = existsPostActions.remove(at: index).action
            await action()
        }
        return result
    }

    func delete(path: String) async throws {
        if respectTaskCancellation, Task.isCancelled { throw CancellationError() }
        if rejectDeleteAfterDisconnect, !isConnected {
            throw RemoteStorageClientError.notConnected
        }
        if !deleteErrorScript.isEmpty { throw deleteErrorScript.removeFirst() }
        deletedPaths.append(path)
        let key = normalize(path)
        // Destroy every alias sharing this blob: on a non-independent backend, deleting the moved-from source
        // takes the moved-to canonical with it.
        for member in aliasGroup(key) {
            nodes[member] = nil
            fileContents[member] = nil
            blobAliases[member] = nil
        }
        directories.remove(key)
    }

    func createDirectory(path: String) async throws {
        if respectTaskCancellation, Task.isCancelled { throw CancellationError() }
        if !createDirectoryErrorScript.isEmpty { throw createDirectoryErrorScript.removeFirst() }
        createdDirectories.append(path)
        directories.insert(normalize(path))
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        if moveAsCopyDelete {
            try await copy(from: sourcePath, to: destinationPath)
            try await delete(path: sourcePath)
            return
        }
        if !moveErrorScript.isEmpty { throw moveErrorScript.removeFirst() }
        if respectTaskCancellation, Task.isCancelled { throw CancellationError() }
        if let hook = onMove {
            await hook(normalize(sourcePath), normalize(destinationPath))
        }
        let src = normalize(sourcePath)
        let dst = normalize(destinationPath)
        movedPaths.append((from: src, to: dst))
        if rejectMoveOntoExistingDestination, nodes[dst] != nil {
            throw NSError(domain: "InMemoryRemoteStorage", code: 17, userInfo: [NSLocalizedDescriptionKey: "destination exists"])
        }
        guard let node = nodes[src] else { throw RemoteErrorFixtures.notFound }
        nodes[dst] = node
        fileContents[dst] = fileContents[src]
        breakAlias(dst)
        if moveMayNotBeIndependentValue {
            // 123pan-style: the source stays, aliased to the destination — one shared blob, not a copy.
            linkAlias(src, dst)
        } else {
            nodes[src] = nil
            fileContents[src] = nil
            breakAlias(src)
        }
        if let index = movePostEffectFailureFromSuffixes.firstIndex(where: { src.hasSuffix($0.suffix) }) {
            throw movePostEffectFailureFromSuffixes.remove(at: index).error
        }
        if !movePostErrorScript.isEmpty { throw movePostErrorScript.removeFirst() }
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        if respectTaskCancellation, Task.isCancelled { throw CancellationError() }
        let src = normalize(sourcePath)
        let dst = normalize(destinationPath)
        copiedPaths.append((from: src, to: dst))
        guard let node = nodes[src] else { throw RemoteErrorFixtures.notFound }
        nodes[dst] = node
        fileContents[dst] = fileContents[src]
        breakAlias(dst)   // a fresh COPY writes an independent blob at dst, breaking any prior alias there
    }

    // MARK: - Helpers

    private func aliasGroup(_ key: String) -> Set<String> {
        blobAliases[key] ?? [key]
    }

    private func linkAlias(_ a: String, _ b: String) {
        let group = aliasGroup(a).union(aliasGroup(b))
        for member in group { blobAliases[member] = group }
    }

    private func breakAlias(_ key: String) {
        guard var group = blobAliases[key] else { return }
        group.remove(key)
        blobAliases[key] = nil
        for member in group { blobAliases[member] = group.count <= 1 ? nil : group }
    }

    private func normalize(_ path: String) -> String {
        "/" + path.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }

    // A directory "exists" if seeded, created, or implied by anything living beneath it.
    private func directoryExists(_ directory: String) -> Bool {
        if directory == "/" { return true }
        if directories.contains(directory) { return true }
        let prefix = directory + "/"
        if nodes.keys.contains(where: { $0.hasPrefix(prefix) }) { return true }
        if directories.contains(where: { $0.hasPrefix(prefix) }) { return true }
        return false
    }

    private func lastComponent(_ key: String) -> String {
        key.split(separator: "/").last.map(String.init) ?? key
    }
}
