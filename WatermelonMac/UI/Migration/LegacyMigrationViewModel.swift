import Combine
import Foundation

@MainActor
final class LegacyMigrationViewModel: ObservableObject {
    enum Phase: Equatable {
        case idle
        case scanning
        case scanned
        case committing
        case committed
        case error(String)
    }

    @Published var legacyFolderPath: String?
    @Published var phase: Phase = .idle
    @Published var report: LegacyScanReport?
    @Published var totals: LegacyImportTotals = .init()
    @Published var currentMonth: LibraryMonthKey?
    @Published var logLines: [String] = []
    @Published private(set) var isClientConnected = false

    let profile: ServerProfileRecord
    private let storageClientFactory: StorageClientFactory
    private let profileStore: ProfileStore
    private(set) var client: (any RemoteStorageClientProtocol)?

    private let planner = LegacyMigrationPlanner()
    private var scanTask: Task<Void, Never>?
    private var commitTask: Task<Void, Never>?
    private var logWriter: ExecutionLogSessionWriter?

    init(
        profile: ServerProfileRecord,
        storageClientFactory: StorageClientFactory,
        profileStore: ProfileStore
    ) {
        self.profile = profile
        self.storageClientFactory = storageClientFactory
        self.profileStore = profileStore
        if let id = profile.id {
            self.legacyFolderPath = profileStore.loadLegacyFolderPath(profileID: id)
        }
    }

    deinit {
        let cToken = client
        Task { await cToken?.disconnect() }
    }

    func connect(password: String) async throws {
        if client != nil {
            isClientConnected = true
            return
        }
        let c = try storageClientFactory.makeClient(profile: profile, password: password)
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
        if let id = profile.id {
            try? profileStore.saveLegacyFolderPath(profileID: id, path: normalized)
        }
    }

    func startScan() {
        guard let path = legacyFolderPath, let client else { return }
        scanTask?.cancel()
        phase = .scanning
        report = nil
        logLines.removeAll()

        scanTask = Task { [weak self, planner] in
            do {
                let result = try await planner.scan(client: client, rootPath: path)
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
        scanTask = nil
        if phase == .scanning { phase = .idle }
    }

    func startCommit() {
        guard let report, let client else { return }
        commitTask?.cancel()
        phase = .committing
        totals = LegacyImportTotals()
        currentMonth = nil
        logLines.removeAll()

        ExecutionLogFileStore.prepareForBackgroundUse()
        let writer = ExecutionLogFileStore.beginSession(kind: .manual)
        let startMessage = "Mac legacy import started · profile=\(profile.name) (\(profile.resolvedStorageType.rawValue)) · source=\(legacyFolderPath ?? "")"
        Task { await writer.appendLog(startMessage, level: .info) }
        logWriter = writer

        let executor = LegacyMigrationExecutor(client: client, profile: profile)
        commitTask = Task { [weak self] in
            let stream = executor.run(report: report)
            for await event in stream {
                await MainActor.run { self?.handle(event: event) }
            }
        }
    }

    func cancelCommit() {
        commitTask?.cancel()
        commitTask = nil
        if phase == .committing { phase = .scanned }
    }

    func resetForNewScan() {
        scanTask?.cancel()
        commitTask?.cancel()
        scanTask = nil
        commitTask = nil
        phase = .idle
        report = nil
        totals = LegacyImportTotals()
        currentMonth = nil
        logLines.removeAll()
    }

    private func handle(event: LegacyImportEvent) {
        switch event {
        case .started(let totals):
            self.totals = totals
            appendLog("Started: \(totals.bundlesPlanned) bundles across \(totals.monthsTotal) months.")
        case .monthStarted(let month, let bundleCount):
            currentMonth = month
            appendLog("→ \(month.text): \(bundleCount) bundle(s)")
        case .bundleResult(_, let bundle, let outcome):
            switch outcome {
            case .imported(let bytes, let copied, let inPlace):
                if copied == 0 {
                    appendLog("  registered fp:\(bundle.assetFingerprint.hexString.prefix(8)) (\(inPlace) already in place)")
                } else if inPlace == 0 {
                    appendLog("  copied fp:\(bundle.assetFingerprint.hexString.prefix(8)) (\(copied) files, \(formatBytes(bytes)))")
                } else {
                    appendLog("  copied fp:\(bundle.assetFingerprint.hexString.prefix(8)) (\(copied) copied + \(inPlace) in-place, \(formatBytes(bytes)))")
                }
            case .skippedFingerprintExists:
                appendLog("  skipped fp:\(bundle.assetFingerprint.hexString.prefix(8)) (already in manifest)")
            case .failed(let reason):
                appendLog("  failed fp:\(bundle.assetFingerprint.hexString.prefix(8)): \(reason)")
            }
        case .monthCompleted(let month):
            appendLog("✔ \(month.text) flushed")
        case .logMessage(let message):
            appendLog(message)
        case .progress(let totals):
            self.totals = totals
        case .finished(let totals):
            self.totals = totals
            currentMonth = nil
            phase = .committed
            appendLog("Finished. imported=\(totals.bundlesImported), skipped(fp)=\(totals.bundlesSkippedFingerprintExists), failed=\(totals.bundlesFailed), copied=\(formatBytes(totals.bytesUploaded)).")
            finalizeLogWriter()
        case .failed(let error, let totals):
            self.totals = totals
            phase = .error(error.localizedDescription)
            appendLog("Failed: \(error.localizedDescription)", level: .error)
            finalizeLogWriter()
        }
    }

    private func appendLog(_ message: String, level: ExecutionLogLevel = .info) {
        logLines.append(message)
        if logLines.count > 500 {
            logLines.removeFirst(logLines.count - 500)
        }
        if let writer = logWriter {
            Task { await writer.appendLog(message, level: level) }
        }
    }

    private func finalizeLogWriter() {
        if let writer = logWriter {
            Task { await writer.finalize() }
        }
        logWriter = nil
    }
}

private func formatBytes(_ bytes: Int64) -> String {
    ByteCountFormatter.fileSizeString(bytes)
}
