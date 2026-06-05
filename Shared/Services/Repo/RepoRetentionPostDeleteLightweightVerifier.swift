import Foundation

enum RepoRetentionPostDeleteVerificationResult: Equatable, Sendable {
    case passed(evidence: RepoRetentionPostDeleteVerificationEvidence)
    case failed(reason: RepoRetentionPostDeleteVerificationFailure, evidence: RepoRetentionPostDeleteVerificationEvidence?)
    case inconclusive(reason: RepoRetentionPostDeleteVerificationInconclusive)
}

struct RepoRetentionPostDeleteVerificationEvidence: Equatable, Sendable {
    let acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo
    let materializedCovered: CoveredRanges
    let observedSeqByWriter: [String: UInt64]
    let observedClock: UInt64
}

enum RepoRetentionPostDeleteVerificationFailure: Equatable, Sendable {
    case missingRepoIdentity(expected: String)
    case repoIdentityMismatch(expected: String, observed: String)
    case missingAcceptedSnapshot(month: LibraryMonthKey)
    case acceptedSnapshotCoverageRegression(filename: String)
    case deletePrefixCoverageRegression(filename: String)
    case stateNotRetentionSuperset
    case coveredRangeRegression
    case observedSeqRegression(writerID: String, expectedAtLeast: UInt64, observed: UInt64)
    case observedClockRegression(expectedAtLeast: UInt64, observed: UInt64)
    case acceptedSnapshotMissingOrTampered(filename: String, expectedSHA: String, observedSHA: String?)
}

enum RepoRetentionPostDeleteVerificationInconclusive: Equatable, Sendable {
    case repoIdentityReadFailed
    case materializerReadRace
    case materializerReadFailed
    case acceptedSnapshotReadFailed(filename: String)
    case deleteTargetStillPresent(path: String)
    case cancelled
}

struct RepoRetentionPostDeleteLightweightVerifier: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = wrapIfSerial(client)
        self.basePath = basePath
    }

    func verify(
        month: LibraryMonthKey,
        expectedRepoID: String,
        contract: RepoRetentionPostDeleteEquivalenceContract
    ) async -> RepoRetentionPostDeleteVerificationResult {
        let repoID = RepoCanonicalIdentity.normalizeLossy(expectedRepoID)
        do {
            switch try await RepoCanonicalIdentityReader(client: client, basePath: basePath).loadCanonicalProvenV2() {
            case .absent:
                return .failed(reason: .missingRepoIdentity(expected: repoID), evidence: nil)
            case .found(let observed):
                let observedRepoID = RepoCanonicalIdentity.normalizeLossy(observed)
                guard observedRepoID == repoID else {
                    return .failed(
                        reason: .repoIdentityMismatch(expected: repoID, observed: observedRepoID),
                        evidence: nil
                    )
                }
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) {
                return .inconclusive(reason: .cancelled)
            }
            return .inconclusive(reason: .repoIdentityReadFailed)
        }

        let snapshotReader = SnapshotReader(client: client, basePath: basePath)
        let snapshotFile: SnapshotFile
        do {
            snapshotFile = try await snapshotReader.read(filename: contract.acceptedSnapshotFilename)
        } catch let error as RepoJSONLReadError {
            switch error {
            case .notFound, .missingHeader, .missingEnd, .integrityMismatch, .decodeFailure:
                return .failed(
                    reason: .acceptedSnapshotMissingOrTampered(
                        filename: contract.acceptedSnapshotFilename,
                        expectedSHA: contract.acceptedSnapshotSHA256Hex.lowercased(),
                        observedSHA: nil
                    ),
                    evidence: nil
                )
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) {
                return .inconclusive(reason: .cancelled)
            }
            return .inconclusive(reason: .acceptedSnapshotReadFailed(filename: contract.acceptedSnapshotFilename))
        }

        guard snapshotFile.sha256Hex.lowercased() == contract.acceptedSnapshotSHA256Hex.lowercased() else {
            return .failed(
                reason: .acceptedSnapshotMissingOrTampered(
                    filename: contract.acceptedSnapshotFilename,
                    expectedSHA: contract.acceptedSnapshotSHA256Hex.lowercased(),
                    observedSHA: snapshotFile.sha256Hex.lowercased()
                ),
                evidence: nil
            )
        }

        let acceptedCovered = snapshotFile.header.covered
        guard acceptedCovered.superset(of: contract.acceptedSnapshotCovered) else {
            return .failed(
                reason: .acceptedSnapshotCoverageRegression(filename: contract.acceptedSnapshotFilename),
                evidence: nil
            )
        }
        guard acceptedCovered.superset(
            of: RetentionInvariantEvaluator.coveredRangesFrom(prefixes: contract.expectedDeletePrefixByWriter)
        ) else {
            return .failed(
                reason: .deletePrefixCoverageRegression(filename: contract.acceptedSnapshotFilename),
                evidence: nil
            )
        }

        let acceptedStillAuthority = await RepoSnapshotCoveredMaxAuthorityChecker.verify(
            client: client,
            basePath: basePath,
            snapshotReader: snapshotReader,
            acceptedCovered: acceptedCovered,
            acceptedFilename: contract.acceptedSnapshotFilename,
            repoID: repoID,
            month: month
        )
        switch acceptedStillAuthority {
        case .confirmed:
            break
        case .cancelled:
            return .inconclusive(reason: .cancelled)
        case .materializerReadFailed:
            return .inconclusive(reason: .materializerReadFailed)
        }

        let parsed = RepoLayout.parseSnapshotFilename(contract.acceptedSnapshotFilename)
        let evidence = RepoRetentionPostDeleteVerificationEvidence(
            acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo(
                filename: contract.acceptedSnapshotFilename,
                month: month,
                lamport: parsed?.lamport ?? 0,
                writerID: snapshotFile.header.writerID,
                runIDPrefix: parsed?.runIDPrefix ?? "",
                covered: acceptedCovered
            ),
            materializedCovered: contract.preDeleteCovered,
            observedSeqByWriter: contract.requiredObservedSeqByWriter,
            observedClock: contract.preDeleteState.observedClock
        )
        return .passed(evidence: evidence)
    }

}
