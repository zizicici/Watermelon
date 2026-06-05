import Foundation

enum RepoRetentionCommitDeleteResult: Equatable, Sendable {
    case preflightBlocked(blockers: [RepoRetentionDeletePreflightBlocker], report: RepoRetentionDeletePreflightReport)
    case completed(
        summary: RepoMetadataDeleteSummary,
        report: RepoRetentionDeletePreflightReport,
        verification: RepoRetentionPostDeleteVerificationResult
    )
    case stopped(
        summary: RepoMetadataDeleteSummary,
        reason: RepoRetentionCommitDeleteStopReason,
        report: RepoRetentionDeletePreflightReport,
        verification: RepoRetentionPostDeleteVerificationResult?
    )
    case verificationFailed(
        summary: RepoMetadataDeleteSummary,
        stopReason: RepoRetentionCommitDeleteStopReason?,
        report: RepoRetentionDeletePreflightReport,
        verification: RepoRetentionPostDeleteVerificationResult
    )
    case verificationInconclusive(
        summary: RepoMetadataDeleteSummary,
        stopReason: RepoRetentionCommitDeleteStopReason?,
        report: RepoRetentionDeletePreflightReport,
        verification: RepoRetentionPostDeleteVerificationResult
    )
}

enum RepoRetentionCommitDeleteStopReason: Equatable, Sendable {
    case preDeleteRevalidationFailed(
        candidate: RepoMetadataDeleteCandidate,
        reason: RepoRetentionCommitDeleteRevalidationFailure
    )
    case deleteFailed(
        candidate: RepoMetadataDeleteCandidate,
        failure: RepoMetadataDeleteFailure
    )
    case cancelled(candidate: RepoMetadataDeleteCandidate?)
}

enum RepoRetentionCommitDeleteRevalidationFailure: Equatable, Sendable {
    case seqZero
    case nonCanonicalPath(expected: String, actual: String)
    case filenameMismatch(expected: String, actual: String)
    case headerMismatch(RepoRetentionCandidateHeaderMismatchReason)
    case contentHashMismatch(expected: String, actual: String)
    case rowCountMismatch(expected: Int, actual: Int)
    case corruptOrUntrusted
    case readFailed
    /// The accepted snapshot body does not retain this commit's asset-level ops, so coverage alone is
    /// not authority to delete it — fail closed rather than drop the last faithful copy.
    case bodyRetentionUnproven
}

struct RepoRetentionCommitDeleteExecutor: Sendable {

    let client: any RemoteStorageClientProtocol
    let basePath: String
    private let policy: RepoCompactionPolicy
    private let isLocalVolume: Bool

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        policy: RepoCompactionPolicy = .default,
        isLocalVolume: Bool
    ) {
        self.client = wrapIfSerial(client)
        self.basePath = basePath
        self.policy = policy
        self.isLocalVolume = isLocalVolume
    }

    func execute(
        plan: RepoRetentionDeletePreflightPlan,
        report: RepoRetentionDeletePreflightReport
    ) async throws -> RepoRetentionCommitDeleteResult {
        let candidates = plan.commitFiles
        var summary = RepoMetadataDeleteSummary(
            month: plan.month,
            repoID: plan.repoID,
            candidateCount: candidates.count
        )
        var stopReason: RepoRetentionCommitDeleteStopReason?
        // The accepted snapshot body (folded pre-delete state for this month) is the only authority that
        // proves a covered commit's data survives its deletion; an under-representing body must block.
        let acceptedMonthState = plan.preDeleteEvidence.postDeleteEquivalenceContract
            .preDeleteState.months[plan.month] ?? .empty

        for candidate in candidates {
            if Task.isCancelled {
                if summary.attempted.isEmpty {
                    throw CancellationError()
                }
                stopReason = .cancelled(candidate: candidate)
                break
            }

            let revalidation: CandidateRevalidation
            do {
                revalidation = try await revalidate(
                    candidate: candidate,
                    expectedRepoID: plan.repoID,
                    acceptedMonthState: acceptedMonthState
                )
            } catch is CancellationError {
                if summary.attempted.isEmpty {
                    throw CancellationError()
                }
                stopReason = .cancelled(candidate: candidate)
                break
            } catch {
                stopReason = .preDeleteRevalidationFailed(candidate: candidate, reason: .readFailed)
                break
            }

            switch revalidation {
            case .valid:
                break
            case .alreadyMissing:
                summary.alreadyMissing.append(candidate)
                continue
            case .failed(let reason):
                stopReason = .preDeleteRevalidationFailed(candidate: candidate, reason: reason)
                break
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
                stopReason = .deleteFailed(candidate: candidate, failure: failure)
                break
            }
        }

        let shouldVerify = stopReason == nil || !summary.attempted.isEmpty || !summary.alreadyMissing.isEmpty
        let verification: RepoRetentionPostDeleteVerificationResult?
        if shouldVerify {
            verification = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath).verify(
                month: plan.month,
                expectedRepoID: plan.repoID,
                contract: plan.preDeleteEvidence.postDeleteEquivalenceContract
            )
        } else {
            verification = nil
        }
        if let verification {
            switch verification {
            case .failed:
                return .verificationFailed(
                    summary: summary,
                    stopReason: stopReason,
                    report: report,
                    verification: verification
                )
            case .inconclusive:
                return .verificationInconclusive(
                    summary: summary,
                    stopReason: stopReason,
                    report: report,
                    verification: verification
                )
            case .passed:
                break
            }
        }
        for candidate in summary.deleted + summary.alreadyMissing {
            do {
                if let _ = try await client.metadata(path: candidate.path) {
                    return .verificationInconclusive(
                        summary: summary,
                        stopReason: stopReason,
                        report: report,
                        verification: .inconclusive(reason: .deleteTargetStillPresent(path: candidate.path))
                    )
                }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                if isStorageNotFoundError(error) { continue }
                return .verificationInconclusive(
                    summary: summary,
                    stopReason: stopReason,
                    report: report,
                    verification: .inconclusive(reason: .deleteTargetStillPresent(path: candidate.path))
                )
            }
        }
        if let stopReason {
            return .stopped(summary: summary, reason: stopReason, report: report, verification: verification)
        }
        guard let verification else {
            return .verificationInconclusive(
                summary: summary,
                stopReason: nil,
                report: report,
                verification: .inconclusive(reason: .materializerReadFailed)
            )
        }
        return .completed(summary: summary, report: report, verification: verification)
    }

    private enum CandidateRevalidation {
        case valid
        case alreadyMissing
        case failed(RepoRetentionCommitDeleteRevalidationFailure)
    }

    private func revalidate(
        candidate: RepoMetadataDeleteCandidate,
        expectedRepoID: String,
        acceptedMonthState: RepoMonthState
    ) async throws -> CandidateRevalidation {
        guard case .commit(let seq) = candidate.kind, seq > 0 else {
            return .failed(.seqZero)
        }
        let expectedFilename = RepoLayout.commitFileName(
            month: candidate.month,
            writerID: candidate.writerID,
            seq: seq
        )
        guard candidate.filename == expectedFilename else {
            return .failed(.filenameMismatch(expected: expectedFilename, actual: candidate.filename))
        }
        guard let parsed = RepoLayout.parseCommitFilename(candidate.filename),
              parsed.month == candidate.month,
              parsed.writerID == candidate.writerID,
              parsed.seq == seq else {
            return .failed(.filenameMismatch(expected: expectedFilename, actual: candidate.filename))
        }
        let expectedPath = RepoLayout.commitFilePath(
            base: basePath,
            month: candidate.month,
            writerID: candidate.writerID,
            seq: seq
        )
        guard candidate.path == expectedPath else {
            return .failed(.nonCanonicalPath(expected: expectedPath, actual: candidate.path))
        }

        let commit: CommitFile
        do {
            commit = try await CommitLogReader(client: client, basePath: basePath).read(remotePath: candidate.path)
        } catch let error as RepoJSONLReadError {
            switch error {
            case .notFound:
                return .alreadyMissing
            case .missingHeader, .missingEnd, .integrityMismatch(_), .decodeFailure(_):
                return .failed(.corruptOrUntrusted)
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .failed(.readFailed)
        }

        if let mismatch = headerMismatch(candidate: candidate, seq: seq, header: commit.header, expectedRepoID: expectedRepoID) {
            return .failed(.headerMismatch(mismatch))
        }
        if commit.ops.contains(where: { $0.clock >= LamportClock.maxAdoptableValue }) {
            return .failed(.corruptOrUntrusted)
        }
        if commit.sha256Hex.lowercased() != candidate.sha256Hex.lowercased() {
            return .failed(.contentHashMismatch(
                expected: candidate.sha256Hex.lowercased(),
                actual: commit.sha256Hex.lowercased()
            ))
        }
        if commit.rowCount != candidate.rowCount {
            return .failed(.rowCountMismatch(expected: candidate.rowCount, actual: commit.rowCount))
        }
        guard RepoBodyRetention.retainsCommit(commit, in: acceptedMonthState) else {
            return .failed(.bodyRetentionUnproven)
        }
        return .valid
    }

    private func headerMismatch(
        candidate: RepoMetadataDeleteCandidate,
        seq: UInt64,
        header: CommitHeader,
        expectedRepoID: String
    ) -> RepoRetentionCandidateHeaderMismatchReason? {
        if RepoCanonicalIdentity.normalizeLossy(header.repoID) != expectedRepoID {
            return .repoID(
                expected: expectedRepoID,
                actual: RepoCanonicalIdentity.normalizeLossy(header.repoID)
            )
        }
        if CommitHeader.parseMonthScope(header.scope) != candidate.month {
            return .month(expected: candidate.month, actual: CommitHeader.parseMonthScope(header.scope))
        }
        if header.writerID != candidate.writerID {
            return .writerID(expected: candidate.writerID, actual: header.writerID)
        }
        if header.seq != seq {
            return .seq(expected: seq, actual: header.seq)
        }
        return nil
    }
}
