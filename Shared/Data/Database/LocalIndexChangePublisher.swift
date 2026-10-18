import Foundation
import GRDB

final class LocalIndexChangePublisher: @unchecked Sendable {
    enum Change: Sendable {
        case touched(assetIDs: Set<String>)
        case bulkInvalidation
    }

    struct Delta: Sendable {
        let revision: UInt64
        let change: Change
    }

    private let changes = ChangeSignal<Void>()
    private let lock = NSLock()
    private var revision: UInt64 = 0
    private var historyFloor: UInt64 = 0
    private var assetRevisions: [String: UInt64] = [:]
    private static let historyLimit = 8_192
    private let observer: Observer

    init(database: Database) throws {
        observer = try Observer(database: database)
        observer.onCommit = { [weak self] change in self?.record(change) }
        database.add(transactionObserver: observer)
    }

    var currentRevision: UInt64 { lock.withLock { revision } }

    func delta(since baseRevision: UInt64) -> Delta {
        lock.withLock {
            guard baseRevision != revision else {
                return Delta(revision: revision, change: .touched(assetIDs: []))
            }
            let change: Change = baseRevision < historyFloor
                ? .bulkInvalidation
                : .touched(assetIDs: Set(assetRevisions.compactMap { $0.value > baseRevision ? $0.key : nil }))
            return Delta(revision: revision, change: change)
        }
    }

    // Commit observers must schedule database reads and UI work outside the writer queue.
    @discardableResult
    func addObserver(_ block: @escaping @Sendable () -> Void) -> UUID {
        changes.addObserver(block)
    }

    @discardableResult
    func addCoalescedMainActorObserver(_ block: @escaping @MainActor @Sendable () -> Void) -> UUID {
        changes.addCoalescedMainActorObserver(block)
    }

    func removeObserver(_ id: UUID) { changes.removeObserver(id) }

    private func record(_ change: Change) {
        lock.withLock {
            revision &+= 1
            switch change {
            case .touched(let ids) where ids.count <= Self.historyLimit:
                for id in ids { assetRevisions[id] = revision }
                if assetRevisions.count <= Self.historyLimit { break }
                let expired = assetRevisions.sorted { $0.value < $1.value }.prefix(assetRevisions.count - Self.historyLimit / 2)
                for (id, changedAt) in expired {
                    historyFloor = max(historyFloor, changedAt)
                    assetRevisions[id] = nil
                }
            case .touched, .bulkInvalidation:
                assetRevisions.removeAll()
                historyFloor = revision
            }
        }
        changes.publish()
    }

    private final class Observer: TransactionObserver {
        var onCommit: ((Change) -> Void)?
        private var identifiersByRowID: [Int64: String]
        private var changedRowIDs = Set<Int64>()
        private var needsIdentityReload = false

        init(database: Database) throws {
            identifiersByRowID = try Self.loadIdentities(database)
        }

        private static func loadIdentities(_ db: Database) throws -> [Int64: String] {
            Dictionary(uniqueKeysWithValues: try Row.fetchAll(db,
                sql: "SELECT rowid, assetLocalIdentifier FROM local_assets WHERE assetFingerprint IS NOT NULL")
                .map { ($0[0] as Int64, $0[1] as String) })
        }

        func observes(eventsOfKind eventKind: DatabaseEventKind) -> Bool {
            switch eventKind {
            case .insert(let table), .delete(let table):
                return table == "local_assets"
            case .update(let table, let columns):
                return table == "local_assets" && !columns.isDisjoint(with: ["assetLocalIdentifier", "assetFingerprint", "updatedAt"])
            }
        }

        func databaseDidChange(with event: DatabaseEvent) {
            changedRowIDs.insert(event.rowID)
        }

        func databaseDidCommit(_ db: Database) {
            guard !changedRowIDs.isEmpty else { return }
            let rowIDs = Array(changedRowIDs)
            changedRowIDs.removeAll()
            do {
                if needsIdentityReload {
                    identifiersByRowID = try Self.loadIdentities(db)
                    needsIdentityReload = false
                    onCommit?(.bulkInvalidation)
                    return
                }
                var nextIdentities: [Int64: String] = [:]
                for start in stride(from: 0, to: rowIDs.count, by: 400) {
                    let batch = Array(rowIDs[start ..< min(start + 400, rowIDs.count)])
                    let placeholders = Array(repeating: "?", count: batch.count).joined(separator: ",")
                    for row in try Row.fetchAll(db,
                        sql: "SELECT rowid, assetLocalIdentifier FROM local_assets WHERE rowid IN (\(placeholders)) AND assetFingerprint IS NOT NULL",
                        arguments: StatementArguments(batch)) {
                        nextIdentities[row[0]] = row[1]
                    }
                }
                var ids = Set(rowIDs.compactMap { identifiersByRowID[$0] })
                ids.formUnion(nextIdentities.values)
                for rowID in rowIDs { identifiersByRowID[rowID] = nextIdentities[rowID] }
                if !ids.isEmpty { onCommit?(.touched(assetIDs: ids)) }
            } catch {
                needsIdentityReload = true
                onCommit?(.bulkInvalidation)
            }
        }

        func databaseDidRollback(_ db: Database) { changedRowIDs.removeAll() }
    }
}
