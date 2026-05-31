import Foundation

/// New V2 error wrapper types must add a switch case in `walk`; classifier callers reach them through here.
nonisolated enum BackupErrorChain {
    enum WalkDecision { case stop; case `continue` }

    static func walk(_ error: Error, body: (Error) -> WalkDecision) {
        var pending: [Error] = [error]
        var visited: Set<ObjectIdentifier> = []
        while let next = pending.popLast() {
            let nsError = next as NSError
            guard visited.insert(ObjectIdentifier(nsError)).inserted else { continue }
            if body(next) == .stop { return }
            switch next {
            case let flush as V2MonthSession.FlushError:
                switch flush {
                case .concurrentFlushRejected:
                    break
                case .postCommitFailed(let underlying):
                    pending.append(underlying)
                }
            case let deferred as V2MonthSession.MonthDurableCommitPartial:
                pending.append(deferred.flushError)
            case let write as SnapshotWriter.WriteError:
                switch write {
                case .ioFailure(let inner), .finalizationFailed(let inner):
                    pending.append(inner)
                case .verificationFailed:
                    break
                }
            case let commit as CommitLogWriter.WriteError:
                switch commit {
                case .ioFailure(let inner):
                    pending.append(inner)
                case .alreadyExists, .encodingFailed:
                    break
                }
            case let gate as MetadataCreateGate.Error:
                switch gate {
                case .stagingVerificationFailed(_, let underlying),
                     .finalVerificationFailed(_, let underlying):
                    if let underlying { pending.append(underlying) }
                case .nonExclusiveFinalization:
                    break
                }
            case let storage as RemoteStorageClientError:
                switch storage {
                case .underlying(let inner):
                    pending.append(inner)
                case .notConnected, .unavailable, .invalidConfiguration,
                     .externalStorageUnavailable, .unsupportedStorageType:
                    break
                }
            default:
                if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
                    pending.append(underlying)
                }
            }
        }
    }

    static func contains(_ error: Error, where predicate: (Error) -> Bool) -> Bool {
        var found = false
        walk(error) { node in
            if predicate(node) {
                found = true
                return .stop
            }
            return .continue
        }
        return found
    }

    static func firstSatisfying(_ error: Error, where predicate: (Error) -> Bool) -> Error? {
        var match: Error?
        walk(error) { node in
            if predicate(node) {
                match = node
                return .stop
            }
            return .continue
        }
        return match
    }

    static func firstOfType<T>(_ error: Error, as type: T.Type) -> T? {
        var match: T?
        walk(error) { node in
            if let typed = node as? T {
                match = typed
                return .stop
            }
            return .continue
        }
        return match
    }

    /// The single NSError collector for the whole repo: descends every V2 wrapper type via `walk`,
    /// so per-classifier copies don't each re-implement (and diverge on) chain traversal.
    /// `walk` dedups by ObjectIdentifier — stricter than domain#code, so it never drops a distinct node.
    static func nsErrorChain(_ error: Error) -> [NSError] {
        var collected: [NSError] = []
        walk(error) { node in
            collected.append(node as NSError)
            return .continue
        }
        return collected
    }
}
