import Foundation

enum RepoSnapshotDeleteRevalidationFailure: Equatable, Sendable {
    case nonCanonicalPath(expected: String, actual: String)
    case filenameMismatch(expected: String, actual: String)
    case headerMismatch(RepoSnapshotCandidateHeaderMismatchReason)
    case contentHashMismatch(expected: String, actual: String)
    case rowCountMismatch(expected: Int, actual: Int)
    case corruptOrUntrusted
    case readFailed
    case nowProtected(filename: String)
    /// The accepted snapshot body does not retain this dominated snapshot's asset-level rows, so
    /// covered-range domination alone is not authority to delete it — fail closed.
    case bodyRetentionUnproven
}

enum RepoSnapshotDeleteStopReason: Equatable, Sendable {
    case preDeleteRevalidationFailed(
        candidate: RepoMetadataDeleteCandidate,
        reason: RepoSnapshotDeleteRevalidationFailure
    )
    case deleteFailed(
        candidate: RepoMetadataDeleteCandidate,
        failure: RepoMetadataDeleteFailure
    )
    case cancelled(candidate: RepoMetadataDeleteCandidate?)
}

enum RepoSnapshotGCResult: Equatable, Sendable {
    case preflightBlocked(
        blockers: [RepoSnapshotDeletePreflightBlocker],
        report: RepoSnapshotDeletePreflightReport
    )
    case completed(
        summary: RepoMetadataDeleteSummary,
        report: RepoSnapshotDeletePreflightReport,
        verification: RepoSnapshotPostDeleteVerificationResult
    )
    case stopped(
        summary: RepoMetadataDeleteSummary,
        reason: RepoSnapshotDeleteStopReason,
        report: RepoSnapshotDeletePreflightReport,
        verification: RepoSnapshotPostDeleteVerificationResult?
    )
    case verificationFailed(
        summary: RepoMetadataDeleteSummary,
        stopReason: RepoSnapshotDeleteStopReason?,
        report: RepoSnapshotDeletePreflightReport,
        verification: RepoSnapshotPostDeleteVerificationResult
    )
    case verificationInconclusive(
        summary: RepoMetadataDeleteSummary,
        stopReason: RepoSnapshotDeleteStopReason?,
        report: RepoSnapshotDeletePreflightReport,
        verification: RepoSnapshotPostDeleteVerificationResult
    )
}

struct RepoSnapshotDeleteExecutor: Sendable {

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
        plan: RepoSnapshotDeletePreflightPlan,
        report: RepoSnapshotDeletePreflightReport
    ) async throws -> RepoSnapshotGCResult {
        let candidates = plan.snapshotsToDelete
        // Covered-range domination is not authority to delete a dominated snapshot whose body the accepted
        // baseline body does not retain; prove retention against the folded pre-delete state or fail closed.
        let acceptedMonthState = plan.postDeleteContract.preDeleteState.months[plan.month] ?? .empty

        let transaction = RepoMetadataDeleteTransaction(client: client)
        let (summary, stopReason): (RepoMetadataDeleteSummary, RepoSnapshotDeleteStopReason?) =
            try await transaction.run(
                candidates: candidates,
                summary: RepoMetadataDeleteSummary(
                    month: plan.month,
                    repoID: plan.repoID,
                    candidateCount: candidates.count
                ),
                preDeleteGuard: { candidate in
                    plan.protectedFilenames.contains(candidate.filename)
                        ? .preDeleteRevalidationFailed(
                            candidate: candidate,
                            reason: .nowProtected(filename: candidate.filename)
                        )
                        : nil
                },
                revalidate: { (candidate: RepoMetadataDeleteCandidate) async throws
                    -> RepoMetadataDeleteTransaction.CandidateRevalidation<RepoSnapshotDeleteStopReason> in
                    switch try await self.revalidate(
                        candidate: candidate,
                        expectedRepoID: plan.repoID,
                        acceptedMonthState: acceptedMonthState
                    ) {
                    case .valid:
                        return .valid
                    case .alreadyMissing:
                        return .alreadyMissing
                    case .failed(let reason):
                        return .stop(.preDeleteRevalidationFailed(candidate: candidate, reason: reason))
                    }
                },
                cancelledStop: { .cancelled(candidate: $0) },
                readFailedStop: { .preDeleteRevalidationFailed(candidate: $0, reason: .readFailed) },
                deleteFailedStop: { .deleteFailed(candidate: $0, failure: $1) }
            )

        let shouldVerify = stopReason == nil
            || !summary.attempted.isEmpty
            || !summary.alreadyMissing.isEmpty
        let verification: RepoSnapshotPostDeleteVerificationResult? = shouldVerify
            ? await RepoSnapshotPostDeleteLightweightVerifier(client: client, basePath: basePath).verify(
                month: plan.month,
                expectedRepoID: plan.repoID,
                contract: plan.postDeleteContract
            )
            : nil
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
        if let stillPresent = try await transaction.firstStillPresentTarget(in: summary.deleted + summary.alreadyMissing) {
            return .verificationInconclusive(
                summary: summary,
                stopReason: stopReason,
                report: report,
                verification: .inconclusive(reason: .deleteTargetStillPresent(path: stillPresent))
            )
        }
        if let stopReason {
            return .stopped(
                summary: summary,
                reason: stopReason,
                report: report,
                verification: verification
            )
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
        case failed(RepoSnapshotDeleteRevalidationFailure)
    }

    private func revalidate(
        candidate: RepoMetadataDeleteCandidate,
        expectedRepoID: String,
        acceptedMonthState: RepoMonthState
    ) async throws -> CandidateRevalidation {
        guard case .snapshot(let lamport, let runIDPrefix) = candidate.kind,
              let parsed = RepoLayout.parseSnapshotFilename(candidate.filename),
              parsed.month == candidate.month,
              parsed.writerID == candidate.writerID,
              parsed.lamport == lamport,
              parsed.runIDPrefix == runIDPrefix else {
            return .failed(.filenameMismatch(
                expected: RepoLayout.snapshotFileName(
                    month: candidate.month,
                    lamport: candidate.snapshotLamport ?? 0,
                    writerID: candidate.writerID,
                    runID: candidate.snapshotRunIDPrefix ?? ""
                ),
                actual: candidate.filename
            ))
        }
        let expectedPath = RepoLayout.normalize(joining: [
            basePath,
            RepoLayout.watermelonDirectory,
            RepoLayout.snapshotsDirectory,
            candidate.filename
        ])
        guard candidate.path == expectedPath else {
            return .failed(.nonCanonicalPath(expected: expectedPath, actual: candidate.path))
        }

        let snapshotFile: SnapshotFile
        do {
            snapshotFile = try await SnapshotReader(client: client, basePath: basePath)
                .read(filename: candidate.filename)
        } catch let error as RepoJSONLReadError {
            switch error {
            case .notFound:
                return .alreadyMissing
            case .missingHeader, .missingEnd, .integrityMismatch, .decodeFailure:
                return .failed(.corruptOrUntrusted)
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .failed(.readFailed)
        }

        if let mismatch = headerMismatch(
            candidate: candidate,
            header: snapshotFile.header,
            expectedRepoID: expectedRepoID
        ) {
            return .failed(.headerMismatch(mismatch))
        }
        if snapshotFile.sha256Hex.lowercased() != candidate.sha256Hex.lowercased() {
            return .failed(.contentHashMismatch(
                expected: candidate.sha256Hex.lowercased(),
                actual: snapshotFile.sha256Hex.lowercased()
            ))
        }
        if snapshotFile.rowCount != candidate.rowCount {
            return .failed(.rowCountMismatch(expected: candidate.rowCount, actual: snapshotFile.rowCount))
        }
        guard RepoBodyRetention.retainsSnapshotBody(snapshotFile, in: acceptedMonthState) else {
            return .failed(.bodyRetentionUnproven)
        }
        return .valid
    }

    private func headerMismatch(
        candidate: RepoMetadataDeleteCandidate,
        header: SnapshotHeader,
        expectedRepoID: String
    ) -> RepoSnapshotCandidateHeaderMismatchReason? {
        if RepoCanonicalIdentity.normalizeLossy(header.repoID) != expectedRepoID {
            return .repoID(
                expected: expectedRepoID,
                actual: RepoCanonicalIdentity.normalizeLossy(header.repoID)
            )
        }
        if CommitHeader.parseMonthScope(header.scope) != candidate.month {
            return .month(
                expected: candidate.month,
                actual: CommitHeader.parseMonthScope(header.scope)
            )
        }
        if header.writerID != candidate.writerID {
            return .writerID(expected: candidate.writerID, actual: header.writerID)
        }
        return nil
    }
}
