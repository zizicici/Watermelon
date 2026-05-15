import Foundation

/// Per-(path, hash) status of a remote resource. Replaces the bool / Set
/// combinations previously scattered across V2MonthSession,
/// RemoteIndexSyncService, and RepoVerifyMonthService.
enum RemoteResourcePresence: Sendable, Equatable {
    /// Listing returned the path with size matching the manifest, but content hash is not SHA-verified.
    case listedSizeMatched

    /// Listing returned the path, size matched, and SHA verified against the stored hash.
    case hashVerified

    /// Confirmed missing — listing didn't include the path, or returned a different size.
    case missing

    /// Status undetermined; the reason drives downstream policy.
    case inconclusive(InconclusiveReason)

    enum InconclusiveReason: Sendable, Equatable {
        /// Pre-verify state: manifest knows the path but no probe has run yet.
        case neverProbed
        /// Per-month verify budget hit (file count or byte cap) before reaching this resource.
        case verifyBudgetExhausted
        /// Verify probe failed with a transient error; retry may succeed.
        case probeFailure
    }
}

/// Per-month presence view. Single source of truth for "is this remote
/// resource trustworthy" — V2 worker session, sync overlay, and verify probe
/// all read and write the same shape.
struct RemoteMonthPresenceMap: Sendable, Equatable {
    private(set) var byPath: [String: RemoteResourcePresence]

    init(byPath: [String: RemoteResourcePresence] = [:]) {
        self.byPath = byPath
    }

    // MARK: - Per-path queries

    /// True only when the bytes have been SHA-verified. Use for any decision that
    /// would be wrong if the file actually held different content.
    func isHashVerified(_ path: String) -> Bool {
        if case .hashVerified = byPath[path] { return true }
        return false
    }

    /// True when the listing identified the path with the right size OR we hash-verified it.
    /// Use to pick a candidate for download / dedup; do NOT use as a content-equality assertion.
    func isUsableCandidate(_ path: String) -> Bool {
        switch byPath[path] {
        case .listedSizeMatched, .hashVerified: return true
        case .missing, .inconclusive, .none: return false
        }
    }

    /// True when listing or verify confirmed the path is gone / size-mismatched.
    func isMissing(_ path: String) -> Bool {
        if case .missing = byPath[path] { return true }
        return false
    }

    func presence(for path: String) -> RemoteResourcePresence? {
        byPath[path]
    }

    // MARK: - Per-hash queries

    /// Hashes whose every known path is `.missing`. Caller supplies the path-by-hash
    /// projection because the presence map is path-keyed by design.
    func fullyMissingHashes(pathsByHash: [Data: Set<String>]) -> Set<Data> {
        var result: Set<Data> = []
        for (hash, paths) in pathsByHash where !paths.isEmpty {
            if paths.allSatisfy({ isMissing($0) }) {
                result.insert(hash)
            }
        }
        return result
    }

    // MARK: - Map-level state

    /// True when no path is `.inconclusive(_)`. Drives overlay freshness signalling.
    var isFullyResolved: Bool {
        byPath.values.allSatisfy { presence in
            if case .inconclusive = presence { return false }
            return true
        }
    }

    var isEmpty: Bool { byPath.isEmpty }

    // MARK: - Mutation

    mutating func mark(path: String, _ presence: RemoteResourcePresence) {
        byPath[path] = presence
    }

    mutating func clear(path: String) {
        byPath.removeValue(forKey: path)
    }
}
