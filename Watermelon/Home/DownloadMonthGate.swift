import Foundation

@MainActor
final class DownloadMonthGate {
    private struct Waiter {
        let id: UUID
        let continuation: CheckedContinuation<Void, Error>
    }

    private var occupied = false
    private var waiters: [Waiter] = []

    func acquire(shouldDrain: @escaping @Sendable () -> Bool) async throws {
        try Task.checkCancellation()
        if shouldDrain() { throw CancellationError() }
        guard occupied else {
            occupied = true
            return
        }
        let id = UUID()
        let monitor = Task {
            while !Task.isCancelled {
                if shouldDrain() {
                    cancelWaiter(id)
                    return
                }
                do { try await Task.sleep(for: .milliseconds(50)) } catch { return }
            }
        }
        defer { monitor.cancel() }
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                if Task.isCancelled || shouldDrain() {
                    continuation.resume(throwing: CancellationError())
                } else {
                    waiters.append(Waiter(id: id, continuation: continuation))
                }
            }
        } onCancel: {
            Task { @MainActor in self.cancelWaiter(id) }
        }
    }

    func release() {
        if waiters.isEmpty {
            occupied = false
        } else {
            waiters.removeFirst().continuation.resume()
        }
    }

    private func cancelWaiter(_ id: UUID) {
        guard let index = waiters.firstIndex(where: { $0.id == id }) else { return }
        waiters.remove(at: index).continuation.resume(throwing: CancellationError())
    }
}
