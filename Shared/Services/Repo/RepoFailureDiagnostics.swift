import Foundation

nonisolated final class RepoFailureDiagnostics: @unchecked Sendable {
    enum Issue: String, Sendable {
        case versionInvalid, versionUnreadable, versionMissing, unexpectedEntry, monthPathConflict
        case recoveryReadFailed, recoveryInvalid, recoveryAmbiguous
        case recoveryWriteFailed, recoveryVerificationFailed, ownershipUnverified
        case operationFailed, monthListChanged
    }

    struct Entry: Sendable, Equatable {
        let issue: Issue
        let stage: String
        let path: String?
        let causes: [String]
        let message: String?
        let detail: String?
        let elapsedSeconds: TimeInterval?
        let underlyingDetails: [String]
        var recovered = false
        var handled = false
    }

    @TaskLocal static var current: RepoFailureDiagnostics?
    private let lock = NSLock()
    private var entries: [Entry] = []

    static func record(_ issue: Issue, stage: String, path: String? = nil, error: Error? = nil,
                       detail: String? = nil, elapsedSeconds: TimeInterval? = nil) {
        guard let current else { return }
        let entry = Entry(issue: issue, stage: stage, path: path, causes: error.map(errorCodes) ?? [],
                          message: error?.localizedDescription, detail: detail.map { String($0.prefix(500)) },
                          elapsedSeconds: elapsedSeconds, underlyingDetails: error.map(errorDetails) ?? [])
        current.append([entry])
    }

    private func append(_ newEntries: [Entry], recovered: Bool = false, handled: Bool = false) {
        lock.withLock {
            for var entry in newEntries {
                entry.recovered = entry.recovered || recovered
                entry.handled = entry.handled || handled
                guard !entries.contains(entry) else { continue }
                entries.append(entry)
            }
            if entries.count > 32 { entries.removeFirst(entries.count - 32) }
        }
    }

    func snapshot() -> [Entry] {
        lock.withLock { entries }
    }

    static func captureRecovery<T>(isSuccessful: (T) -> Bool = { _ in true }, failureIsHandled: Bool = false,
                                   operation: () async throws -> T) async rethrows -> T {
        guard let parent = current else { return try await operation() }
        let diagnostics = RepoFailureDiagnostics()
        do {
            let result = try await $current.withValue(diagnostics, operation: operation)
            let recovered = isSuccessful(result)
            parent.append(diagnostics.snapshot(), recovered: recovered, handled: !recovered && failureIsHandled)
            return result
        } catch {
            parent.append(diagnostics.snapshot(), handled: failureIsHandled)
            throw error
        }
    }

    static func capture<T>(stage: String, path: String? = nil, detail: String? = nil,
                           operation: () async throws -> T) async throws -> T {
        let start = Date()
        do { return try await operation() }
        catch {
            if RemoteFaultLite.classify(error) != .cancelled {
                record(.operationFailed, stage: stage, path: path, error: error,
                       detail: detail, elapsedSeconds: Date().timeIntervalSince(start))
            }
            throw error
        }
    }

    static func errorCodes(_ error: Error) -> [String] {
        var result: [String] = []
        var next: Error? = error
        while let current = next, result.count < 8 {
            if let remote = current as? RemoteStorageClientError, case .underlying(let underlying) = remote {
                next = underlying
                continue
            }
            if let exhausted = current as? RemoteReadBackRetryExhaustedError {
                result.append("RemoteReadBackRetryExhausted")
                next = exhausted.underlying
                continue
            }
            if let probe = current as? RemoteProbeDeferredCleanupError {
                result.append("RemoteProbeDeferredCleanup")
                next = probe.underlyingError
                continue
            }
            let ns = current as NSError
            if let lite = current as? LiteRepoError {
                result.append("LiteRepoError/\(lite.diagnosticCode)")
            } else if let storage = current as? RemoteStorageClientError {
                result.append("RemoteStorageClientError/\(storage.diagnosticCode)")
            } else {
                result.append("\(ns.domain)/\(ns.code)")
            }
            next = ns.userInfo[NSUnderlyingErrorKey] as? Error
        }
        return result
    }

    static func errorDetails(_ error: Error) -> [String] {
        var result: [String] = []
        var next: Error? = error
        for _ in 0..<8 {
            guard let current = next else { break }
            let error = StorageErrorMessages.unwrapped(current)
            let ns = error as NSError
            result.append("original_message: \(error.localizedDescription)")
            let keys = [S3ErrorClassifier.userInfoServerCodeKey, S3ErrorClassifier.userInfoServerMessageKey,
                        OneDriveErrorClassifier.codeKey, DropboxErrorClassifier.userInfoSummaryKey,
                        DropboxErrorClassifier.userInfoTagPathKey, DropboxErrorClassifier.userInfoEndpointKey,
                        DropboxErrorClassifier.userInfoUserMessageKey, DropboxErrorClassifier.userInfoRequestIDKey,
                        GoogleDriveErrorClassifier.userInfoReasonsKey, GoogleDriveErrorClassifier.userInfoMessageKey]
            for key in keys {
                if let value = ns.userInfo[key] { result.append("\(key): \(value)") }
            }
            if let detail = SFTPErrorClassifier.diagnosticDetail(error) { result.append("transport_detail: \(detail)") }
            if let readBack = error as? RemoteReadBackRetryExhaustedError {
                next = readBack.underlying
            } else {
                next = ns.userInfo[NSUnderlyingErrorKey] as? Error
            }
        }
        return result
    }
}

nonisolated struct StorageConnectionFailure: LocalizedError {
    let underlying: Error
    let diagnostics: [RepoFailureDiagnostics.Entry]
    var errorDescription: String? { underlying.localizedDescription }

    static func capture<T>(stage: String, path: String? = nil, operation: () async throws -> T) async throws -> T {
        let diagnostics = RepoFailureDiagnostics.current ?? RepoFailureDiagnostics()
        do {
            return try await RepoFailureDiagnostics.$current.withValue(diagnostics) {
                try await RepoFailureDiagnostics.capture(stage: stage, path: path, operation: operation)
            }
        } catch {
            if Task.isCancelled { throw CancellationError() }
            if error is StorageConnectionFailure || RemoteFaultLite.classify(error) == .cancelled { throw error }
            throw StorageConnectionFailure(underlying: error, diagnostics: diagnostics.snapshot())
        }
    }
}

extension LiteRepoError {
    var diagnosticCode: String {
        switch self {
        case .repoDamaged: return "repoDamaged"
        case .repoUnsupported: return "repoUnsupported"
        case .repoMaintenanceUnavailable: return "repoMaintenanceUnavailable"
        case .probeFault(let category, _): return "probeFault.\(category)"
        case .lockConflict: return "lockConflict"
        case .localWriteInProgress: return "localWriteInProgress"
        case .ownLockConflict: return "ownLockConflict"
        case .lockFault(let category): return "lockFault.\(category)"
        case .writerIdentityUnavailable: return "writerIdentityUnavailable"
        case .versionCommitFailed: return "versionCommitFailed"
        case .leaseConfidenceLost: return "leaseConfidenceLost"
        case .ownershipLost: return "ownershipLost"
        case .existingLiteManifestConflict: return "existingLiteManifestConflict"
        case .v1MonthManifestUnreadable: return "v1MonthManifestUnreadable"
        case .v1SourceChangedDuringMigration: return "v1SourceChangedDuringMigration"
        case .upgradeMonthUnavailable: return "upgradeMonthUnavailable"
        case .upgradeMonthPathConflict: return "upgradeMonthPathConflict"
        case .upgradeMonthsChanged: return "upgradeMonthsChanged"
        }
    }
}
