import Foundation

struct LegacyImportLogEntry: Identifiable, Equatable {
    let id: Int
    let message: String
}

@MainActor
final class LegacyMigrationViewModel {
    enum Phase: Equatable {
        case idle
        case scanning
        case scanned
        case committing
        case committed
        case error(String)
    }

    var onChange: (() -> Void)?
    var onRepositoryChanged: (() -> Void)?

    var legacyFolderPath: String? {
        didSet { notifyChange() }
    }
    var phase: Phase = .idle {
        didSet { notifyChange() }
    }
    var report: LegacyScanReport? {
        didSet { notifyChange() }
    }
    var totals: LegacyImportTotals = .init() {
        didSet { notifyChange() }
    }
    var currentMonth: LibraryMonthKey? {
        didSet { notifyChange() }
    }
    var logLines: [LegacyImportLogEntry] = [] {
        didSet { notifyChange() }
    }
    var replaceSubsetAssets: Bool = false {
        didSet { notifyChange() }
    }
    var skipPerceptualDuplicates: Bool = true {
        didSet { notifyChange() }
    }
    private(set) var isClientConnected = false {
        didSet { notifyChange() }
    }

    private var nextLogId: Int = 0

    let profile: ServerProfileRecord
    private let storageClientFactory: StorageClientFactory
    private let profileStore: ProfileStore
    private let appRuntimeFlags: AppRuntimeFlags
    private(set) var client: (any RemoteStorageClientProtocol)?
    private var credentialPayload = ""

    private let planner = LegacyMigrationPlanner()
    private var scanTask: Task<Void, Never>?
    private var commitTask: Task<Void, Never>?
    private var logWriter: ExecutionLogSessionWriter?
    private var shouldRefreshRemoteSnapshot = false

    var isRunning: Bool {
        scanTask != nil || commitTask != nil
    }

    init(
        profile: ServerProfileRecord,
        storageClientFactory: StorageClientFactory,
        profileStore: ProfileStore,
        appRuntimeFlags: AppRuntimeFlags
    ) {
        self.profile = profile
        self.storageClientFactory = storageClientFactory
        self.profileStore = profileStore
        self.appRuntimeFlags = appRuntimeFlags
        if let id = profile.id {
            self.legacyFolderPath = profileStore.loadLegacyFolderPath(profileID: id)
        }
    }

    deinit {
        scanTask?.cancel()
        commitTask?.cancel()
        let cToken = client
        Task { await cToken?.disconnect() }
    }

    func connect(password: String) async throws {
        credentialPayload = password
        if client != nil {
            isClientConnected = true
            return
        }
        let c = try storageClientFactory.makeClient(profile: profile, credentialPayload: password)
        try await c.connect()
        client = c
        isClientConnected = true
    }

    func setLegacyPath(_ path: String) {
        let normalized = RemotePathBuilder.normalizePath(path)
        legacyFolderPath = normalized
        phase = .idle
        report = nil
        logLines.removeAll()
        nextLogId = 0
        if let id = profile.id {
            try? profileStore.saveLegacyFolderPath(profileID: id, path: normalized)
        }
    }

    func startScan() {
        guard !isRunning,
              let path = legacyFolderPath,
              let client else {
            return
        }
        phase = .scanning
        report = nil
        logLines.removeAll()
        nextLogId = 0

        let targetBase = profile.basePath
        let dedup = skipPerceptualDuplicates
        scanTask = Task { [weak self, planner] in
            defer { self?.finishScanTask() }
            do {
                let result = try await planner.scan(
                    client: client,
                    rootPath: path,
                    targetBasePath: targetBase,
                    enablePerceptualDedup: dedup
                )
                await MainActor.run {
                    self?.report = result
                    self?.phase = .scanned
                }
            } catch is CancellationError {
                await MainActor.run { self?.phase = .idle }
            } catch {
                await MainActor.run { self?.phase = .error(error.localizedDescription) }
            }
        }
    }

    func cancelScan() {
        scanTask?.cancel()
    }

    func startCommit() {
        guard !isRunning, let report, let client else { return }
        guard appRuntimeFlags.tryEnterExecution() else {
            phase = .error(
                String(
                    localized: "mac.maintenance.busyMessage",
                    defaultValue: "Another backup or maintenance operation is already in progress."
                )
            )
            return
        }
        appRuntimeFlags.setExecutionCancellationHandler(for: self) {
            $0.cancelCommit()
        }
        phase = .committing
        totals = LegacyImportTotals()
        currentMonth = nil
        logLines.removeAll()
        nextLogId = 0
        shouldRefreshRemoteSnapshot = false

        ExecutionLogFileStore.prepareForBackgroundUse()
        let writer = ExecutionLogFileStore.beginSession(kind: .manual)
        let startMessage = String(
            format: String(
                localized: "migration.log.sessionStart"
            ),
            profile.name,
            profile.resolvedStorageType.rawValue,
            legacyFolderPath ?? "",
            replaceSubsetAssets
                ? String(localized: "common.yes")
                : String(localized: "common.no")
        )
        logWriter = writer

        let options = LegacyMigrationOptions(
            replaceSubsetAssets: replaceSubsetAssets
        )
        let profile = profile
        let credentialPayload = credentialPayload
        let storageClientFactory = storageClientFactory
        let appRuntimeFlags = appRuntimeFlags
        commitTask = Task { [weak self] in
            defer {
                appRuntimeFlags.exitExecution()
                self?.finishCommitTask()
            }
            await writer.appendLog(startMessage, level: .info)
            let lockClientHandle: LiteLockClientHandle?
            do {
                if profile.resolvedStorageType == .externalVolume {
                    lockClientHandle = nil
                } else {
                    let lockClient = try storageClientFactory.makeClient(
                        profile: profile,
                        credentialPayload: credentialPayload
                    )
                    try await lockClient.connect()
                    lockClientHandle = LiteLockClientHandle(
                        client: lockClient
                    )
                }
            } catch {
                await self?.handle(
                    event: .failed(
                        error: error,
                        totals: LegacyImportTotals()
                    )
                )
                return
            }
            let executor = LegacyMigrationExecutor(
                client: client,
                profile: profile,
                lockClientHandle: lockClientHandle
            )
            let stream = executor.run(report: report, options: options)
            for await event in stream {
                await self?.handle(event: event)
            }
            await self?.finalizeLogWriter()
        }
    }

    func cancelCommit() {
        commitTask?.cancel()
    }

    func resetForNewScan() {
        guard !isRunning else { return }
        scanTask?.cancel()
        commitTask?.cancel()
        scanTask = nil
        commitTask = nil
        phase = .idle
        report = nil
        totals = LegacyImportTotals()
        currentMonth = nil
        logLines.removeAll()
        nextLogId = 0
    }

    private func finishScanTask() {
        scanTask = nil
        if phase == .scanning {
            phase = .idle
        } else {
            notifyChange()
        }
    }

    private func finishCommitTask() {
        commitTask = nil
        if phase == .committing {
            phase = .scanned
        } else {
            notifyChange()
        }
        if shouldRefreshRemoteSnapshot {
            shouldRefreshRemoteSnapshot = false
            onRepositoryChanged?()
        }
    }

    private func handle(event: LegacyImportEvent) async {
        switch event {
        case .started(let totals):
            self.totals = totals
            await appendLog(
                String(
                    format: String(
                        localized: "migration.log.started"
                    ),
                    totals.bundlesPlanned,
                    totals.monthsTotal
                )
            )
        case .monthStarted(let month, let bundleCount):
            currentMonth = month
            await appendLog(
                String(
                    format: String(
                        localized: "migration.log.monthStarted"
                    ),
                    month.text,
                    bundleCount
                )
            )
        case .bundleResult(_, let bundle, let outcome):
            let fingerprint = String(
                bundle.assetFingerprint.hexString.prefix(8)
            )
            switch outcome {
            case .imported(let bytes, let copied, let inPlace):
                if copied == 0 {
                    await appendLog(
                        String(
                            format: String(
                                localized:
                                    "migration.log.bundleRegistered"
                            ),
                            fingerprint,
                            inPlace
                        )
                    )
                } else if inPlace == 0 {
                    await appendLog(
                        String(
                            format: String(
                                localized:
                                    "migration.log.bundleCopied"
                            ),
                            fingerprint,
                            copied,
                            formatBytes(bytes)
                        )
                    )
                } else {
                    await appendLog(
                        String(
                            format: String(
                                localized:
                                    "migration.log.bundleCopiedMixed"
                            ),
                            fingerprint,
                            copied,
                            inPlace,
                            formatBytes(bytes)
                        )
                    )
                }
            case .skippedFingerprintExists:
                await appendLog(
                    String(
                        format: String(
                            localized:
                                "migration.log.bundleSkipped"
                        ),
                        fingerprint
                    )
                )
            case .failed(let reason):
                await appendLog(
                    String(
                        format: String(
                            localized:
                                "migration.log.bundleFailed"
                        ),
                        fingerprint,
                        reason
                    ),
                    level: .error
                )
            }
        case .monthCompleted(let month):
            await appendLog(
                String(
                    format: String(
                        localized: "migration.log.monthFlushed"
                    ),
                    month.text
                )
            )
        case .monthFailed(let month, let reason):
            await appendLog(
                String(
                    format: String(
                        localized:
                            "migration.log.monthFlushFailed"
                    ),
                    month.text,
                    reason
                ),
                level: .error
            )
        case .logMessage(let message):
            await appendLog(message)
        case .progress(let totals):
            self.totals = totals
        case .finished(let totals):
            self.totals = totals
            currentMonth = nil
            recordRepositoryChange(from: totals)
            if totals.monthsFailed > 0 {
                phase = .error(
                    String(
                        format: String(
                            localized:
                                "migration.log.finishedWithFailures"
                        ),
                        totals.monthsFailed
                    )
                )
            } else {
                phase = .committed
            }
            await appendLog(
                String(
                    format: String(
                        localized: "migration.log.finished"
                    ),
                    totals.bundlesImported,
                    totals.bundlesSkippedFingerprintExists,
                    totals.bundlesFailed,
                    totals.monthsFailed,
                    formatBytes(totals.bytesUploaded)
                )
            )
            await finalizeLogWriter()
        case .cancelled(let totals):
            self.totals = totals
            currentMonth = nil
            recordRepositoryChange(from: totals)
            phase = .scanned
            await finalizeLogWriter()
        case .failed(let error, let totals):
            self.totals = totals
            currentMonth = nil
            recordRepositoryChange(from: totals)
            phase = .error(error.localizedDescription)
            await appendLog(
                String(
                    format: String(
                        localized: "migration.log.failed"
                    ),
                    error.localizedDescription
                ),
                level: .error
            )
            await finalizeLogWriter()
        }
    }

    private func recordRepositoryChange(
        from totals: LegacyImportTotals
    ) {
        shouldRefreshRemoteSnapshot =
            shouldRefreshRemoteSnapshot
            || LegacyMigrationTerminalPolicy
                .shouldRefreshRemoteSnapshot(after: totals)
    }

    private func appendLog(
        _ message: String,
        level: ExecutionLogLevel = .info
    ) async {
        nextLogId += 1
        logLines.append(LegacyImportLogEntry(id: nextLogId, message: message))
        if let writer = logWriter {
            await writer.appendLog(message, level: level)
        }
    }

    private func finalizeLogWriter() async {
        if let writer = logWriter {
            await writer.finalize()
        }
        logWriter = nil
    }

    private func notifyChange() {
        onChange?()
    }
}

private func formatBytes(_ bytes: Int64) -> String {
    ByteCountFormatter.fileSizeString(bytes)
}
