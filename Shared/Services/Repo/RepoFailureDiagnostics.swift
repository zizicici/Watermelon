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
    }

    @TaskLocal static var current: RepoFailureDiagnostics?
    private let lock = NSLock()
    private var entries: [Entry] = []

    static func record(_ issue: Issue, stage: String, path: String? = nil, error: Error? = nil, detail: String? = nil) {
        guard let current else { return }
        let entry = Entry(issue: issue, stage: stage, path: path, causes: error.map(errorCodes) ?? [],
                          message: error?.localizedDescription, detail: detail.map { String($0.prefix(500)) })
        current.lock.withLock {
            // Probes re-inspect the same directory, so an identical record adds nothing.
            guard !current.entries.contains(entry) else { return }
            current.entries.append(entry)
            if current.entries.count > 32 { current.entries.removeFirst(current.entries.count - 32) }
        }
    }

    func snapshot() -> [Entry] {
        lock.withLock { entries }
    }

    static func capture<T>(stage: String, path: String? = nil, operation: () async throws -> T) async throws -> T {
        do { return try await operation() }
        catch {
            if RemoteFaultLite.classify(error) != .cancelled {
                record(.operationFailed, stage: stage, path: path, error: error)
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
            let ns = current as NSError
            if let lite = current as? LiteRepoError {
                result.append("LiteRepoError/\(lite.diagnosticCode)")
            } else {
                result.append("\(ns.domain)/\(ns.code)")
            }
            next = ns.userInfo[NSUnderlyingErrorKey] as? Error
        }
        return result
    }
}

nonisolated struct StorageConnectionFailure: LocalizedError {
    let underlying: Error
    let diagnostics: [RepoFailureDiagnostics.Entry]
    var errorDescription: String? { underlying.localizedDescription }
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
