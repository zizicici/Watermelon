import AppKit
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

    @Published var sourceFolderURL: URL?
    @Published var phase: Phase = .idle
    @Published var report: LegacyScanReport?
    @Published var totals: LegacyImportTotals = .init()
    @Published var currentMonth: LibraryMonthKey?
    @Published var logLines: [String] = []

    private let storageClientFactory: StorageClientFactory
    private let profileStore: ProfileStore?
    private let profileID: Int64?
    private let planner = LegacyMigrationPlanner()
    private var scanTask: Task<Void, Never>?
    private var commitTask: Task<Void, Never>?
    private var logWriter: ExecutionLogSessionWriter?

    init(
        storageClientFactory: StorageClientFactory,
        profileStore: ProfileStore? = nil,
        profileID: Int64? = nil
    ) {
        self.storageClientFactory = storageClientFactory
        self.profileStore = profileStore
        self.profileID = profileID
        if let profileStore, let profileID,
           let restored = profileStore.resolveLegacySource(profileID: profileID) {
            self.sourceFolderURL = restored
        }
    }

    func pickSourceFolder() {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.message = "Select a folder containing legacy data to import"
        panel.prompt = String(localized: "common.choose")
        if panel.runModal() == .OK, let url = panel.url {
            sourceFolderURL = url
            phase = .idle
            report = nil
            logLines.removeAll()
            if let profileStore, let profileID {
                try? profileStore.saveLegacySource(profileID: profileID, url: url)
            }
        }
    }

    func startScan() {
        guard let url = sourceFolderURL else { return }
        scanTask?.cancel()
        phase = .scanning
        report = nil
        logLines.removeAll()

        scanTask = Task { [weak self, planner] in
            let didStart = url.startAccessingSecurityScopedResource()
            defer {
                if didStart { url.stopAccessingSecurityScopedResource() }
            }
            do {
                let result = try await planner.scan(rootURL: url)
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

    func startCommit(profile: ServerProfileRecord, password: String) {
        guard let report else { return }
        guard let url = sourceFolderURL else { return }
        commitTask?.cancel()
        phase = .committing
        totals = LegacyImportTotals()
        currentMonth = nil
        logLines.removeAll()

        ExecutionLogFileStore.prepareForBackgroundUse()
        let writer = ExecutionLogFileStore.beginSession(kind: .manual)
        let startMessage = "Mac legacy import started · profile=\(profile.name) (\(profile.resolvedStorageType.rawValue)) · source=\(url.path)"
        Task { await writer.appendLog(startMessage, level: .info) }
        logWriter = writer

        let session = LegacyImportSession(sourceURL: url)
        let executor = LegacyMigrationExecutor(
            storageClientFactory: storageClientFactory,
            session: session
        )

        commitTask = Task { [weak self] in
            let stream = executor.run(report: report, profile: profile, password: password)
            for await event in stream {
                await MainActor.run {
                    self?.handle(event: event)
                }
            }
        }
    }

    func cancelCommit() {
        commitTask?.cancel()
        commitTask = nil
        if phase == .committing {
            phase = .scanned
        }
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
            appendLog("Started import: \(totals.bundlesPlanned) bundles across \(totals.monthsTotal) months.")
        case .monthStarted(let month, let bundleCount):
            currentMonth = month
            appendLog("→ \(month.text): \(bundleCount) bundle(s)")
        case .bundleResult(_, let bundle, let outcome):
            switch outcome {
            case .imported(let bytes, let uploaded, let skipped):
                if skipped == 0 {
                    appendLog("  imported fp:\(bundle.assetFingerprint.hexString.prefix(8)) (\(uploaded) files, \(formatBytes(bytes)))")
                } else {
                    appendLog("  imported fp:\(bundle.assetFingerprint.hexString.prefix(8)) (\(uploaded) new + \(skipped) hash-existed, \(formatBytes(bytes)))")
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
            appendLog("Finished. imported=\(totals.bundlesImported), skipped(fp)=\(totals.bundlesSkippedFingerprintExists), failed=\(totals.bundlesFailed), uploaded=\(formatBytes(totals.bytesUploaded)).")
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
