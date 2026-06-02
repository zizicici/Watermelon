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
    /// Byte-exact path identity: a `[String:…]` map folds NFC/NFD twins (see `RemotePhysicalPathKey`),
    /// collapsing two distinct resources' presence into one entry on exact-name backends.
    private(set) var byPath: [RemotePhysicalPathKey: RemoteResourcePresence]

    init(byPath: [String: RemoteResourcePresence] = [:]) {
        self.byPath = Dictionary(uniqueKeysWithValues: byPath.map { (RemotePhysicalPathKey($0.key), $0.value) })
    }

    /// Use SHA verification for decisions that would be wrong if bytes differ.
    func isHashVerified(_ path: String) -> Bool {
        if case .hashVerified = byPath[RemotePhysicalPathKey(path)] { return true }
        return false
    }

    /// Size-matched listings are usable candidates but not content-equality proof.
    func isUsableCandidate(_ path: String) -> Bool {
        switch byPath[RemotePhysicalPathKey(path)] {
        case .listedSizeMatched, .hashVerified: return true
        case .missing, .inconclusive, .none: return false
        }
    }

    func isMissing(_ path: String) -> Bool {
        if case .missing = byPath[RemotePhysicalPathKey(path)] { return true }
        return false
    }

    func presence(for path: String) -> RemoteResourcePresence? {
        byPath[RemotePhysicalPathKey(path)]
    }

    /// Caller supplies paths-by-hash because the presence map is path-keyed by design.
    /// Byte-exact keys so a same-hash NFC/NFD twin pair isn't folded to one path before the all-missing test.
    func fullyMissingHashes(pathsByHash: [Data: Set<RemotePhysicalPathKey>]) -> Set<Data> {
        var result: Set<Data> = []
        for (hash, paths) in pathsByHash where !paths.isEmpty {
            if paths.allSatisfy({ isMissing($0.path) }) {
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
        byPath[RemotePhysicalPathKey(path)] = presence
    }

    mutating func clear(path: String) {
        byPath.removeValue(forKey: RemotePhysicalPathKey(path))
    }
}
