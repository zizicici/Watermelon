import Foundation
import os.log

private let upgradeLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoUpgrade")

struct RepoUpgradeExecution: Sendable {
    let workerCount: Int
    let makeClient: (@Sendable () throws -> any RemoteStorageClientProtocol)?

    init(
        workerCount: Int = 1,
        makeClient: (@Sendable () throws -> any RemoteStorageClientProtocol)? = nil
    ) {
        self.workerCount = max(1, workerCount)
        self.makeClient = makeClient
    }

    func run(
        months: [LibraryMonthKey],
        client: any RemoteStorageClientProtocol,
        operation: @escaping @Sendable (LibraryMonthKey, any RemoteStorageClientProtocol) async throws -> Void,
        onMonthCompleted: @escaping @Sendable (Int) async -> Void
    ) async throws {
        let count = min(workerCount, months.count)
        guard let makeClient, count > 1 else {
            if count > 1 {
                upgradeLog.warning("Repo upgrade requested \(count) workers without a client factory; using one worker")
            }
            for (index, month) in months.enumerated() {
                try Task.checkCancellation()
                try await operation(month, client)
                await onMonthCompleted(index + 1)
            }
            return
        }

        let pool = StorageClientPool(maxConnections: count - 1, makeClient: makeClient)
        do {
            try await withThrowingTaskGroup(of: Int.self) { group in
                func enqueue(_ month: LibraryMonthKey, worker: Int) {
                    group.addTask {
                        try Task.checkCancellation()
                        let workerClient = worker == 0 ? client : try await pool.acquire()
                        do {
                            try Task.checkCancellation()
                            try await operation(month, workerClient)
                        } catch {
                            if worker != 0 { await pool.release(workerClient, reusable: true) }
                            throw error
                        }
                        if worker != 0 { await pool.release(workerClient, reusable: true) }
                        return worker
                    }
                }

                for worker in 0..<count {
                    enqueue(months[worker], worker: worker)
                }
                var next = count
                var completed = 0
                while let worker = try await group.next() {
                    try Task.checkCancellation()
                    completed += 1
                    await onMonthCompleted(completed)
                    if next < months.count {
                        enqueue(months[next], worker: worker)
                        next += 1
                    }
                }
            }
        } catch {
            await pool.shutdown()
            throw error
        }
        await pool.shutdown()
    }
}
