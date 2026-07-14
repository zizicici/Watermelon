import Foundation

// Capability for destructive remote deletes under a proven write lease. `deleteOrphan` re-proves ownership
// on EVERY attempt (not once per batch), so a lease lost mid-cleanup can't delete a content-addressed file
// another writer has since re-referenced (which would corrupt that writer's manifest). Construct only from
// an active RepoWriteMode + its data client — that's the whole point: you can't delete without the lease.
struct LeasedRemoteWriter {
    let client: any RemoteStorageClientProtocol
    let mode: RepoWriteMode

    enum Outcome: Equatable {
        case deleted
        case leaseLost   // ownership no longer provable — the caller should stop; remaining files leak (safe)
        case cancelled
        case failed      // transient/other failure on this one file — a bounded leak; other files may proceed
    }

    // Deletes one now-orphaned resource file with a per-attempt write-tier lease proof + bounded retry.
    func deleteOrphan(path: String, attempts: Int = 3) async -> Outcome {
        for attempt in 0 ..< attempts {
            if Task.isCancelled { return .cancelled }
            do {
                try await RepoWriteGuard.assertControlWriteAllowed(mode)
            } catch is CancellationError {
                return .cancelled
            } catch {
                return .leaseLost
            }
            do {
                try await client.delete(path: path)
                return .deleted
            } catch is CancellationError {
                return .cancelled
            } catch {
                if attempt == attempts - 1 { return .failed }
                try? await Task.sleep(nanoseconds: 500_000_000)
            }
        }
        return .failed
    }
}
