import Foundation

enum RemoteResourcePresence: Sendable, Equatable {
    case listedSizeMatched

    case hashVerified

    case missing

    case inconclusive(InconclusiveReason)

    enum InconclusiveReason: Sendable, Equatable {
        case neverProbed
        case verifyBudgetExhausted
        case probeFailure
    }
}

struct RemoteMonthPresenceMap: Sendable, Equatable {
    private(set) var byPath: [String: RemoteResourcePresence]

    init(byPath: [String: RemoteResourcePresence] = [:]) {
        self.byPath = byPath
    }

    /// Use SHA verification for decisions that would be wrong if bytes differ.
    func isHashVerified(_ path: String) -> Bool {
        if case .hashVerified = byPath[path] { return true }
        return false
    }

    /// Size-matched listings are usable candidates but not content-equality proof.
    func isUsableCandidate(_ path: String) -> Bool {
        switch byPath[path] {
        case .listedSizeMatched, .hashVerified: return true
        case .missing, .inconclusive, .none: return false
        }
    }

    func isMissing(_ path: String) -> Bool {
        if case .missing = byPath[path] { return true }
        return false
    }

    func presence(for path: String) -> RemoteResourcePresence? {
        byPath[path]
    }

    /// Caller supplies paths-by-hash because the presence map is path-keyed by design.
    func fullyMissingHashes(pathsByHash: [Data: Set<String>]) -> Set<Data> {
        var result: Set<Data> = []
        for (hash, paths) in pathsByHash where !paths.isEmpty {
            if paths.allSatisfy({ isMissing($0) }) {
                result.insert(hash)
            }
        }
        return result
    }

    /// Drives overlay freshness signalling.
    var isFullyResolved: Bool {
        byPath.values.allSatisfy { presence in
            if case .inconclusive = presence { return false }
            return true
        }
    }

    var isEmpty: Bool { byPath.isEmpty }

    mutating func mark(path: String, _ presence: RemoteResourcePresence) {
        byPath[path] = presence
    }

    mutating func clear(path: String) {
        byPath.removeValue(forKey: path)
    }
}
