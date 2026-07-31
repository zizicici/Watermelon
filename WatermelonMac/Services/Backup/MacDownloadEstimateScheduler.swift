import Foundation

@MainActor
final class MacDownloadEstimateScheduler {
    typealias Operation = @MainActor @Sendable () async -> Int64?
    typealias ValueHandler = @MainActor @Sendable (Int64?) -> Void

    private var task: Task<Void, Never>?
    private var generation: UInt64 = 0

    func schedule(
        operation: @escaping Operation,
        onValue: @escaping ValueHandler
    ) {
        cancel()
        let expectedGeneration = generation
        task = Task { [weak self] in
            let value = await operation()
            guard !Task.isCancelled,
                  let self,
                  generation == expectedGeneration else {
                return
            }
            task = nil
            onValue(value)
        }
    }

    func cancel() {
        generation &+= 1
        task?.cancel()
        task = nil
    }

    deinit {
        task?.cancel()
    }
}
