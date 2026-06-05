import Foundation

/// Read-only probe for committed V2 payload directories. Bootstrap identity guards and the V1 verify
/// path both need to know whether `.watermelon/commits` or `.watermelon/snapshots` already hold real
/// data before minting a repoID or refusing a V1 verify; this is the single fail-closed definition.
nonisolated enum RepoV2DataProbe {
    /// True when `commits/` or `snapshots/` contains a non-staging entry. Our own in-flight
    /// `<commit|snapshot>.staging-<uuid>` writes are ignored (maintenance sweeps them after open); any
    /// other unexpected name still counts so admission fails closed as damaged.
    static func hasAnyCommitOrSnapshotData(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Bool {
        let probes: [(subdir: String, isFinalDataName: (String) -> Bool)] = [
            (RepoLayout.commitsDirectory, { RepoLayout.parseCommitFilename($0) != nil }),
            (RepoLayout.snapshotsDirectory, { RepoLayout.parseSnapshotFilename($0) != nil })
        ]
        for probe in probes {
            let path = RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory, probe.subdir])
            do {
                let entries = try await client.list(path: path)
                if entries.contains(where: { !isOwnStagingArtifact($0.name, ofFinalDataName: probe.isFinalDataName) }) {
                    return true
                }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                if isStorageNotFoundError(error) { continue }
                throw error
            }
        }
        return false
    }

    private static func isOwnStagingArtifact(
        _ name: String,
        ofFinalDataName isFinalDataName: (String) -> Bool
    ) -> Bool {
        guard let range = name.range(of: ".staging-") else { return false }
        return isFinalDataName(String(name[..<range.lowerBound]))
    }
}
