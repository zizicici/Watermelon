import Foundation
import os.log

// Shared probe for a Lite month's data directory (<YYYY>/<MM>). Lite keeps month truth under
// .watermelon/months; the data directory is a separate, layout-shared resource tree. The three call
// sites that read it before a reconcile — MonthManifestStore.loadOrCreate, .loadSeeded, and
// RemoteIndexSyncService.verifyMonth — must read it the same way: a confirmed-absent directory is an
// empty listing, any other transport fault fails closed (never read as "zero files"), and a destructive
// prune (whole-month clear or large-ratio) of a non-empty manifest needs a second confirming listing
// before it is allowed to dirty the manifest.
nonisolated enum LiteDataDirectoryProbe {
    private static let log = Logger(subsystem: "com.zizicici.watermelon", category: "LiteDataDirectoryProbe")

    // Fraction of a month's known data files whose disappearance in one listing is treated as a
    // destructive prune needing confirmation. Whole-month clearing (empty listing) always qualifies.
    static let largePruneRatio = 0.5

    struct Listing {
        let entries: [RemoteStorageEntry]
        let directoryMissing: Bool

        var dataFileNames: Set<String> {
            Set(entries
                .filter { !$0.isDirectory && $0.name != MonthManifestStore.manifestFileName }
                .map(\.name))
        }
    }

    enum PruneConfirmation: Equatable {
        case reconcile(fileNames: Set<String>, directoryMissing: Bool)
        case skip
    }

    // Single authoritative LIST. A confirmed-absent directory (notFound) collapses to an empty listing
    // flagged `directoryMissing`. Any other fault (offline / share down / backend) throws so the caller
    // fails closed instead of reading the absence as "zero data files".
    static func probe(
        client: any RemoteStorageClientProtocol,
        monthAbsolutePath: String
    ) async throws -> Listing {
        do {
            let entries = try await client.list(path: monthAbsolutePath)
            return Listing(entries: entries, directoryMissing: false)
        } catch {
            if RemoteFaultLite.classify(error) == .notFound {
                return Listing(entries: [], directoryMissing: true)
            }
            throw error
        }
    }

    // Guards a destructive reconcile prune against a transient empty/missing listing. A non-destructive
    // prune trusts the initial listing. A destructive one (whole-month clear or >= largePruneRatio of the
    // manifest) is allowed only when a second LIST reproduces the first exactly; a confirmation that
    // faults or disagrees yields `.skip`, leaving the non-empty manifest intact for a later run.
    static func confirmPrune(
        client: any RemoteStorageClientProtocol,
        monthAbsolutePath: String,
        initial: Listing,
        manifestFileNames: Set<String>
    ) async -> PruneConfirmation {
        let listed = initial.dataFileNames
        guard isDestructivePrune(listed: listed, manifest: manifestFileNames) else {
            return .reconcile(fileNames: listed, directoryMissing: initial.directoryMissing)
        }

        let confirmation: Listing
        do {
            confirmation = try await probe(client: client, monthAbsolutePath: monthAbsolutePath)
        } catch {
            log.error("[LiteDataDirectoryProbe] skipped destructive prune at \(monthAbsolutePath, privacy: .public): confirmation LIST faulted")
            return .skip
        }
        guard confirmation.dataFileNames == listed else {
            log.error("[LiteDataDirectoryProbe] skipped destructive prune at \(monthAbsolutePath, privacy: .public): confirmation LIST disagreed")
            return .skip
        }
        return .reconcile(fileNames: confirmation.dataFileNames, directoryMissing: confirmation.directoryMissing)
    }

    private static func isDestructivePrune(listed: Set<String>, manifest: Set<String>) -> Bool {
        guard !manifest.isEmpty else { return false }
        let pruneCount = manifest.subtracting(listed).count
        guard pruneCount > 0 else { return false }
        if listed.isEmpty { return true }   // whole-month clear
        return Double(pruneCount) >= Double(manifest.count) * largePruneRatio
    }
}
