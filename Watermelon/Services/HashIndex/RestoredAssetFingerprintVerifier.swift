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
        expectedFingerprint: Data
    ) async throws -> Bool {
        for attempt in 0...delays.count {
            try Task.checkCancellation()
            let result = try await buildIndex([assetLocalIdentifier])
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
                try await Task.sleep(for: delays[attempt])
            }
        }
        return false
    }
}
