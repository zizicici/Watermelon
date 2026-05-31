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

struct RepoRetentionPostDeleteVerifier: Sendable {
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

        let output: RepoMaterializer.MaterializeOutput
        do {
            output = try await RepoMaterializer(client: client, basePath: basePath)
                .materializeMonth(month, expectedRepoID: repoID)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) {
                return .inconclusive(reason: .cancelled)
            }
            if error is RepoMaterializer.MetadataReadRaceError {
                return .inconclusive(reason: .materializerReadRace)
            }
            return .inconclusive(reason: .materializerReadFailed)
        }

        guard let acceptedSnapshot = output.acceptedSnapshotBaselinesByMonth[month] else {
            return .failed(reason: .missingAcceptedSnapshot(month: month), evidence: nil)
        }

        let snapshotReader = SnapshotReader(client: client, basePath: basePath)
        do {
            let file = try await snapshotReader.read(filename: contract.acceptedSnapshotFilename)
            guard file.sha256Hex.lowercased() == contract.acceptedSnapshotSHA256Hex.lowercased() else {
                return .failed(
                    reason: .acceptedSnapshotMissingOrTampered(
                        filename: contract.acceptedSnapshotFilename,
                        expectedSHA: contract.acceptedSnapshotSHA256Hex.lowercased(),
                        observedSHA: file.sha256Hex.lowercased()
                    ),
                    evidence: nil
                )
            }
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

        let evidence = RepoRetentionPostDeleteVerificationEvidence(
            acceptedSnapshot: acceptedSnapshot,
            materializedCovered: output.coveredByMonth[month, default: .empty],
            observedSeqByWriter: output.observedSeqByWriter,
            observedClock: output.state.observedClock
        )
        switch RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: output.state,
            month: month,
            contract: contract
        ) {
        case .passed:
            return .passed(evidence: evidence)
        case .failed(let reason):
            return .failed(reason: reason, evidence: evidence)
        }
    }
}
