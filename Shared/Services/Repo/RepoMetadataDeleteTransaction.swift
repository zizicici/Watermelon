import Foundation

/// Shared delete-transaction plumbing for the two V2 metadata-delete families (commit-prefix GC and
/// snapshot GC). It owns only the mechanics both families already shared verbatim — cancellation gate,
/// optional pre-delete guard, family revalidation dispatch, `RepoMetadataDeleteSummary` mutation, delete
/// error classification, and the post-delete target-presence probe. Eligibility, revalidation rules,
/// post-delete verification, and the public result enums stay family-specific.
struct RepoMetadataDeleteTransaction: Sendable {

    let client: any RemoteStorageClientProtocol

    enum CandidateRevalidation<StopReason> {
        case valid
        case alreadyMissing
        case stop(StopReason)
    }

    /// Runs the shared per-candidate delete loop. Returns the (mutated) summary and the family stop
    /// reason that halted it, or nil when every candidate was processed. Throws `CancellationError`
    /// only when cancellation lands before any delete was attempted; cancellation after an attempted
    /// delete is surfaced through `cancelledStop`/`deleteFailedStop` so the family result path owns it.
    func run<StopReason>(
        candidates: [RepoMetadataDeleteCandidate],
        summary initialSummary: RepoMetadataDeleteSummary,
        preDeleteGuard: (RepoMetadataDeleteCandidate) -> StopReason?,
        revalidate: (RepoMetadataDeleteCandidate) async throws -> CandidateRevalidation<StopReason>,
        cancelledStop: (RepoMetadataDeleteCandidate?) -> StopReason,
        readFailedStop: (RepoMetadataDeleteCandidate) -> StopReason,
        deleteFailedStop: (RepoMetadataDeleteCandidate, RepoMetadataDeleteFailure) -> StopReason
    ) async throws -> (summary: RepoMetadataDeleteSummary, stopReason: StopReason?) {
        var summary = initialSummary
        var stopReason: StopReason?

        for candidate in candidates {
            if Task.isCancelled {
                if summary.attempted.isEmpty {
                    throw CancellationError()
                }
                stopReason = cancelledStop(candidate)
                break
            }

            if let guardStop = preDeleteGuard(candidate) {
                stopReason = guardStop
                break
            }

            let revalidation: CandidateRevalidation<StopReason>
            do {
                revalidation = try await revalidate(candidate)
            } catch is CancellationError {
                if summary.attempted.isEmpty {
                    throw CancellationError()
                }
                stopReason = cancelledStop(candidate)
                break
            } catch {
                stopReason = readFailedStop(candidate)
                break
            }

            switch revalidation {
            case .valid:
                break
            case .alreadyMissing:
                summary.alreadyMissing.append(candidate)
                continue
            case .stop(let reason):
                stopReason = reason
            }
            if stopReason != nil { break }

            summary.attempted.append(candidate)
            do {
                try await client.delete(path: candidate.path)
                summary.deleted.append(candidate)
            } catch {
                let isCancellation = RemoteWriteClassifier.isCancellation(error)
                if !isCancellation, isStorageNotFoundError(error) {
                    summary.alreadyMissing.append(candidate)
                    continue
                }
                let failure: RepoMetadataDeleteFailure = isCancellation
                    ? .cancelled
                    : .other(String(describing: error))
                stopReason = deleteFailedStop(candidate, failure)
                break
            }
        }

        return (summary, stopReason)
    }

    /// Post-delete presence probe shared by both families: returns the path of the first candidate whose
    /// metadata is still present (or whose presence check failed for a non-not-found reason), or nil when
    /// every target is confirmed gone. Cancellation propagates as `CancellationError`.
    func firstStillPresentTarget(
        in candidates: [RepoMetadataDeleteCandidate]
    ) async throws -> String? {
        for candidate in candidates {
            do {
                if let _ = try await client.metadata(path: candidate.path) {
                    return candidate.path
                }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                if isStorageNotFoundError(error) { continue }
                return candidate.path
            }
        }
        return nil
    }
}
