import Foundation

enum RepoRetentionCommitDeleteResult: Equatable, Sendable {
    case preflightBlocked(blockers: [RepoRetentionDeletePreflightBlocker], report: RepoRetentionDeletePreflightReport)
    case completed(
        summary: RepoRetentionCommitDeleteSummary,
        report: RepoRetentionDeletePreflightReport,
        verification: RepoRetentionPostDeleteVerificationResult
    )
    case stopped(
        summary: RepoRetentionCommitDeleteSummary,
        reason: RepoRetentionCommitDeleteStopReason,
        report: RepoRetentionDeletePreflightReport,
        verification: RepoRetentionPostDeleteVerificationResult?
    )
    case verificationFailed(
        summary: RepoRetentionCommitDeleteSummary,
        stopReason: RepoRetentionCommitDeleteStopReason?,
        report: RepoRetentionDeletePreflightReport,
        verification: RepoRetentionPostDeleteVerificationResult
    )
    case verificationInconclusive(
        summary: RepoRetentionCommitDeleteSummary,
        stopReason: RepoRetentionCommitDeleteStopReason?,
        report: RepoRetentionDeletePreflightReport,
        verification: RepoRetentionPostDeleteVerificationResult
    )
}

struct RepoRetentionCommitDeleteSummary: Equatable, Sendable {
    let month: LibraryMonthKey
    let repoID: String
    let candidateCount: Int
    var attempted: [RepoRetentionDeleteCandidate] = []
    var deleted: [RepoRetentionDeleteCandidate] = []
    var alreadyMissing: [RepoRetentionDeleteCandidate] = []

    var attemptedCount: Int { attempted.count }
    var deletedCount: Int { deleted.count }
    var alreadyMissingCount: Int { alreadyMissing.count }
}

enum RepoRetentionCommitDeleteStopReason: Equatable, Sendable {
    case preDeleteRevalidationFailed(
        candidate: RepoRetentionDeleteCandidate,
        reason: RepoRetentionCommitDeleteRevalidationFailure
    )
    case deleteFailed(
        candidate: RepoRetentionDeleteCandidate,
        failure: RepoRetentionCommitDeleteFailure
    )
    case cancelled(candidate: RepoRetentionDeleteCandidate?)
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
}

enum RepoRetentionCommitDeleteFailure: Equatable, Sendable {
    case cancelled
    case other(String)
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
        var summary = RepoRetentionCommitDeleteSummary(
            month: plan.month,
            repoID: plan.repoID,
            candidateCount: candidates.count
        )
        var stopReason: RepoRetentionCommitDeleteStopReason?

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
                revalidation = try await revalidate(candidate: candidate, expectedRepoID: plan.repoID)
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
                let failure: RepoRetentionCommitDeleteFailure = isCancellation
                    ? .cancelled
                    : .other(String(describing: error))
                stopReason = .deleteFailed(candidate: candidate, failure: failure)
                break
            }
        }

        let shouldVerify = stopReason == nil || !summary.attempted.isEmpty || !summary.alreadyMissing.isEmpty
        let verification: RepoRetentionPostDeleteVerificationResult?
        if shouldVerify {
            let lightweight = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath).verify(
                month: plan.month,
                expectedRepoID: plan.repoID,
                contract: plan.preDeleteEvidence.postDeleteEquivalenceContract
            )
            switch lightweight {
            case .passed, .failed:
                verification = lightweight
            case .inconclusive:
                verification = await RepoRetentionPostDeleteVerifier(client: client, basePath: basePath).verify(
                    month: plan.month,
                    expectedRepoID: plan.repoID,
                    contract: plan.preDeleteEvidence.postDeleteEquivalenceContract
                )
            }
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
        candidate: RepoRetentionDeleteCandidate,
        expectedRepoID: String
    ) async throws -> CandidateRevalidation {
        guard candidate.seq > 0 else {
            return .failed(.seqZero)
        }
        let expectedFilename = RepoLayout.commitFileName(
            month: candidate.month,
            writerID: candidate.writerID,
            seq: candidate.seq
        )
        guard candidate.filename == expectedFilename else {
            return .failed(.filenameMismatch(expected: expectedFilename, actual: candidate.filename))
        }
        guard let parsed = RepoLayout.parseCommitFilename(candidate.filename),
              parsed.month == candidate.month,
              parsed.writerID == candidate.writerID,
              parsed.seq == candidate.seq else {
            return .failed(.filenameMismatch(expected: expectedFilename, actual: candidate.filename))
        }
        let expectedPath = RepoLayout.commitFilePath(
            base: basePath,
            month: candidate.month,
            writerID: candidate.writerID,
            seq: candidate.seq
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

        if let mismatch = headerMismatch(candidate: candidate, header: commit.header, expectedRepoID: expectedRepoID) {
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
        return .valid
    }

    private func headerMismatch(
        candidate: RepoRetentionDeleteCandidate,
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
        if header.seq != candidate.seq {
            return .seq(expected: candidate.seq, actual: header.seq)
        }
        return nil
    }
}
