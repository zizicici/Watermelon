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
}

enum RepoSnapshotDeleteFailure: Equatable, Sendable {
    case cancelled
    case other(String)
}

enum RepoSnapshotDeleteStopReason: Equatable, Sendable {
    case preDeleteRevalidationFailed(
        candidate: RepoSnapshotDeleteCandidate,
        reason: RepoSnapshotDeleteRevalidationFailure
    )
    case deleteFailed(
        candidate: RepoSnapshotDeleteCandidate,
        failure: RepoSnapshotDeleteFailure
    )
    case cancelled(candidate: RepoSnapshotDeleteCandidate?)
}

struct RepoSnapshotDeleteSummary: Equatable, Sendable {
    let month: LibraryMonthKey
    let repoID: String
    let candidateCount: Int
    var attempted: [RepoSnapshotDeleteCandidate] = []
    var deleted: [RepoSnapshotDeleteCandidate] = []
    var alreadyMissing: [RepoSnapshotDeleteCandidate] = []

    var attemptedCount: Int { attempted.count }
    var deletedCount: Int { deleted.count }
    var alreadyMissingCount: Int { alreadyMissing.count }
}

enum RepoSnapshotGCResult: Equatable, Sendable {
    case preflightBlocked(
        blockers: [RepoSnapshotDeletePreflightBlocker],
        report: RepoSnapshotDeletePreflightReport
    )
    case completed(
        summary: RepoSnapshotDeleteSummary,
        report: RepoSnapshotDeletePreflightReport,
        verification: RepoSnapshotPostDeleteVerificationResult
    )
    case stopped(
        summary: RepoSnapshotDeleteSummary,
        reason: RepoSnapshotDeleteStopReason,
        report: RepoSnapshotDeletePreflightReport,
        verification: RepoSnapshotPostDeleteVerificationResult?
    )
    case verificationFailed(
        summary: RepoSnapshotDeleteSummary,
        stopReason: RepoSnapshotDeleteStopReason?,
        report: RepoSnapshotDeletePreflightReport,
        verification: RepoSnapshotPostDeleteVerificationResult
    )
    case verificationInconclusive(
        summary: RepoSnapshotDeleteSummary,
        stopReason: RepoSnapshotDeleteStopReason?,
        report: RepoSnapshotDeletePreflightReport,
        verification: RepoSnapshotPostDeleteVerificationResult
    )
}

/// Not safe for concurrent invocation on the same month; caller is responsible.
struct RepoSnapshotDeleteExecutor: Sendable {
    typealias PeerStatusProvider = RepoSnapshotDeletePreflightService.PeerStatusProvider

    let client: any RemoteStorageClientProtocol
    let basePath: String
    private let policy: RepoCompactionPolicy
    private let isLocalVolume: Bool
    private let barrierClockSkewToleranceMs: Int64
    private let peerStatusProvider: PeerStatusProvider

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        policy: RepoCompactionPolicy = .default,
        isLocalVolume: Bool,
        barrierClockSkewToleranceMs: Int64 = 5 * 60 * 1000,
        peerStatusProvider: @escaping PeerStatusProvider = {
            throw RepoRetentionDeletePreflightError.livenessSnapshotUnavailable
        }
    ) {
        self.client = wrapIfSerial(client)
        self.basePath = basePath
        self.policy = policy
        self.isLocalVolume = isLocalVolume
        self.barrierClockSkewToleranceMs = barrierClockSkewToleranceMs
        self.peerStatusProvider = peerStatusProvider
    }

    func execute(
        month: LibraryMonthKey,
        expectedRepoID: String,
        nowMs: Int64
    ) async throws -> RepoSnapshotGCResult {
        try Task.checkCancellation()

        let repoID = RepoCanonicalIdentity.normalizeLossy(expectedRepoID)
        let preflightResult = try await RepoSnapshotDeletePreflightService(
            client: client,
            basePath: basePath,
            policy: policy,
            isLocalVolume: isLocalVolume,
            barrierClockSkewToleranceMs: barrierClockSkewToleranceMs,
            peerStatusProvider: peerStatusProvider
        ).makePlan(month: month, expectedRepoID: repoID, nowMs: nowMs)
        switch preflightResult {
        case .blocked(let blockers, let report):
            return .preflightBlocked(blockers: blockers, report: report)
        case .planned(let plan, let report):
            return try await execute(plan: plan, report: report)
        }
    }

    private func execute(
        plan: RepoSnapshotDeletePreflightPlan,
        report: RepoSnapshotDeletePreflightReport
    ) async throws -> RepoSnapshotGCResult {
        let candidates = plan.snapshotsToDelete
        var summary = RepoSnapshotDeleteSummary(
            month: plan.month,
            repoID: plan.repoID,
            candidateCount: candidates.count
        )
        var stopReason: RepoSnapshotDeleteStopReason?

        for candidate in candidates {
            if Task.isCancelled {
                if summary.attempted.isEmpty {
                    throw CancellationError()
                }
                stopReason = .cancelled(candidate: candidate)
                break
            }

            // Defense-in-depth: refuse to touch any protected snapshot even if scanner
            // produced it as a candidate.
            if plan.protectedFilenames.contains(candidate.filename) {
                stopReason = .preDeleteRevalidationFailed(
                    candidate: candidate,
                    reason: .nowProtected(filename: candidate.filename)
                )
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
                let failure: RepoSnapshotDeleteFailure = isCancellation
                    ? .cancelled
                    : .other(String(describing: error))
                stopReason = .deleteFailed(candidate: candidate, failure: failure)
                break
            }
        }

        let shouldVerify = stopReason == nil
            || !summary.attempted.isEmpty
            || !summary.alreadyMissing.isEmpty
        let verification: RepoSnapshotPostDeleteVerificationResult? = shouldVerify
            ? await RepoSnapshotPostDeleteVerifier(client: client, basePath: basePath).verify(
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
        for candidate in summary.deleted + summary.alreadyMissing {
            do {
                if let _ = try await client.metadata(path: candidate.path) {
                    return .verificationInconclusive(
                        summary: summary,
                        stopReason: nil,
                        report: report,
                        verification: .inconclusive(reason: .deleteTargetStillPresent(path: candidate.path))
                    )
                }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                if isStorageNotFoundError(error) { continue }
                return .verificationInconclusive(
                    summary: summary,
                    stopReason: nil,
                    report: report,
                    verification: .inconclusive(reason: .deleteTargetStillPresent(path: candidate.path))
                )
            }
        }
        return .completed(summary: summary, report: report, verification: verification)
    }

    private enum CandidateRevalidation {
        case valid
        case alreadyMissing
        case failed(RepoSnapshotDeleteRevalidationFailure)
    }

    private func revalidate(
        candidate: RepoSnapshotDeleteCandidate,
        expectedRepoID: String
    ) async throws -> CandidateRevalidation {
        // Filename round-trip equality (catches any non-canonical form).
        guard let parsed = RepoLayout.parseSnapshotFilename(candidate.filename),
              parsed.month == candidate.month,
              parsed.writerID == candidate.writerID,
              parsed.lamport == candidate.lamport,
              parsed.runIDPrefix == candidate.runIDPrefix else {
            return .failed(.filenameMismatch(
                expected: RepoLayout.snapshotFileName(
                    month: candidate.month,
                    lamport: candidate.lamport,
                    writerID: candidate.writerID,
                    runID: candidate.runIDPrefix
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
        return .valid
    }

    private func headerMismatch(
        candidate: RepoSnapshotDeleteCandidate,
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
