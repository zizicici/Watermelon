import Foundation

final class RestoredAssetFingerprintVerifier: @unchecked Sendable {
    typealias BuildFingerprintIndex = @Sendable (_ assetLocalIdentifiers: Set<PhotoKitLocalIdentifier>) async throws -> LocalHashIndexBuildResult
    typealias FetchFingerprintRecords = @Sendable (_ assetLocalIdentifiers: Set<PhotoKitLocalIdentifier>) throws -> [PhotoKitLocalIdentifier: LocalAssetFingerprintRecord]

    // PhotoKit settle window after a Photos save: rebuild + read may race the asset becoming queryable.
    static let defaultDelays: [Duration] = [
        .milliseconds(500),
        .milliseconds(750),
        .milliseconds(1_000),
        .milliseconds(1_500),
        .milliseconds(2_000),
        .milliseconds(2_500),
        .milliseconds(3_000)
    ]

    private let buildIndex: BuildFingerprintIndex
    private let fetchRecords: FetchFingerprintRecords
    private let delays: [Duration]

    init(
        buildIndex: @escaping BuildFingerprintIndex,
        fetchRecords: @escaping FetchFingerprintRecords,
        delays: [Duration] = RestoredAssetFingerprintVerifier.defaultDelays
    ) {
        self.buildIndex = buildIndex
        self.fetchRecords = fetchRecords
        self.delays = delays
    }

    func verifyDurableBinding(
        assetLocalIdentifier: PhotoKitLocalIdentifier,
        expectedFingerprint: AssetFingerprint
    ) async throws -> Bool {
        for attempt in 0...delays.count {
            // The caller invokes this immediately after Photos imported the asset, which is not
            // rolled back on cancellation. The first durable fingerprint build/write must survive a
            // concurrent stop — otherwise the imported asset is left without a hash-index row and
            // re-downloads as a duplicate next session. Run the build detached so caller cancellation
            // can't abort the write; only the inter-attempt settle sleep stays cancellable, so a stop
            // still ends the retry loop after the in-flight write completes.
            let result = try await Task.detached(priority: .utility) { [buildIndex] in
                try await buildIndex([assetLocalIdentifier])
            }.value
            if result.readyAssetIDs.contains(assetLocalIdentifier) {
                // Off the caller thread to avoid blocking on the sync SQLite read.
                let records = try await Task.detached(priority: .utility) { [fetchRecords] in
                    try fetchRecords([assetLocalIdentifier])
                }.value
                if records[assetLocalIdentifier]?.fingerprint == expectedFingerprint {
                    return true
                }
                // PhotoKit may expose a transient resource shape (e.g. Live Photo without
                // the paired video yet); keep retrying until the settle budget is exhausted.
            }
            if attempt < delays.count {
                // Shield the settle wait too: once Photos imported the asset, the bounded retry must
                // run to completion (durable binding written, or settle budget exhausted) even under
                // caller cancellation. A cancellable sleep here would collapse the retry the instant a
                // stop arrives mid-settle — leaving the imported asset unindexed and re-downloadable as
                // a duplicate next session. Detached so the parent's cancellation can't abort it.
                await Self.shieldedSleep(delays[attempt])
            }
        }
        return false
    }

    /// Settle wait that ignores caller cancellation; see `verifyDurableBinding`.
    private static func shieldedSleep(_ duration: Duration) async {
        await Task.detached(priority: .utility) {
            try? await Task.sleep(for: duration)
        }.value
    }
}
