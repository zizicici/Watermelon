import Foundation

@MainActor
final class ScreenBoundAsyncRunner<Output> {
    private let isScreenActive: () -> Bool
    private let onStateChanged: () -> Void
    private var task: Task<Void, Never>?
    private var operationID: UUID?

    var isRunning: Bool { operationID != nil }

    init(
        isScreenActive: @escaping () -> Bool,
        onStateChanged: @escaping () -> Void
    ) {
        self.isScreenActive = isScreenActive
        self.onStateChanged = onStateChanged
    }

    deinit {
        task?.cancel()
    }

    @discardableResult
    func start(
        operation: @escaping () async throws -> Output,
        completion: @escaping (Result<Output, Error>) -> Void
    ) -> Bool {
        guard operationID == nil else { return false }
        let operationID = UUID()
        self.operationID = operationID
        onStateChanged()
        task = Task { [weak self] in
            let result: Result<Output, Error>?
            do {
                let output = try await operation()
                try Task.checkCancellation()
                result = .success(output)
            } catch is CancellationError {
                result = nil
            } catch {
                result = Task.isCancelled ? nil : .failure(error)
            }
            self?.finish(
                operationID: operationID,
                result: result,
                completion: completion
            )
        }
        return true
    }

    func cancel() {
        guard operationID != nil else { return }
        operationID = nil
        let activeTask = task
        task = nil
        activeTask?.cancel()
        onStateChanged()
    }

    private func finish(
        operationID: UUID,
        result: Result<Output, Error>?,
        completion: (Result<Output, Error>) -> Void
    ) {
        guard self.operationID == operationID else { return }
        self.operationID = nil
        task = nil
        onStateChanged()
        guard let result, isScreenActive() else { return }
        completion(result)
    }
}

@MainActor
enum PresentationDismissalSequencer {
    static func waitUntilDismissed(isPresented: @escaping () -> Bool) async {
        while isPresented() {
            await Task.yield()
        }
    }

    static func performAfterDismissal(
        isPresented: @escaping () -> Bool,
        action: () -> Void
    ) async {
        await waitUntilDismissed(isPresented: isPresented)
        action()
    }
}
