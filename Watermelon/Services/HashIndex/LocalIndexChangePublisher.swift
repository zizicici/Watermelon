import Foundation

/// Backup execution intentionally does not publish here — it already drives Home
/// via its own per-month / per-asset callbacks, and a publish would double-fire.
final class LocalIndexChangePublisher: Sendable {
    enum Change: Sendable {
        case touched(assetIDs: Set<String>)
        case bulkInvalidation
    }

    private let changes = ChangeSignal<Change>()

    init() {}

    @discardableResult
    func addObserver(_ block: @escaping @Sendable (Change) -> Void) -> UUID {
        changes.addObserver(block)
    }

    func removeObserver(_ id: UUID) {
        changes.removeObserver(id)
    }

    func publish(_ change: Change) {
        changes.publish(change)
    }
}
