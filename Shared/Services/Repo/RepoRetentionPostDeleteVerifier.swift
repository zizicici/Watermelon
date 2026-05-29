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
    case retainedBarrierCoverageRegression(filename: String)
    case deletePrefixCoverageRegression(filename: String)
    case stateNotRetentionSuperset
    case coveredRangeRegression
    case observedSeqRegression(writerID: String, expectedAtLeast: UInt64, observed: UInt64)
    case observedClockRegression(expectedAtLeast: UInt64, observed: UInt64)
    case retainedBarrierCheckpointMissingOrTampered(filename: String, expectedSHA: String, observedSHA: String?)
    case acceptedSnapshotMissingOrTampered(filename: String, expectedSHA: String, observedSHA: String?)
}

enum RepoRetentionPostDeleteVerificationInconclusive: Equatable, Sendable {
    case repoIdentityReadFailed
    case materializerReadRace
    case materializerReadFailed
    case retainedBarrierCheckpointReadFailed(filename: String)
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
            switch try await RepoCanonicalIdentityReader(client: client, basePath: basePath).loadCanonical() {
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

        // Always re-read the pre-delete accepted baseline SHA. The accepted baseline is
        // protected non-target evidence even when a newer baseline supersedes it post-delete;
        // a storage fault that removes/tampers the pre-delete baseline must fail closed.
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

        // Re-validate retained barrier checkpoint snapshots survived deletion intact. A
        // storage fault during commit delete that also removes/tampers a checkpoint must
        // fail closed even if materialization passes from a newer baseline.
        for (filename, expectedSHA) in contract.retainedBarrierCheckpointSHA256ByFilename
            .sorted(by: { $0.key < $1.key }) {
            let snapshotFile: SnapshotFile
            do {
                snapshotFile = try await snapshotReader.read(filename: filename)
            } catch let error as RepoJSONLReadError {
                switch error {
                case .notFound, .missingHeader, .missingEnd, .integrityMismatch, .decodeFailure:
                    return .failed(
                        reason: .retainedBarrierCheckpointMissingOrTampered(
                            filename: filename,
                            expectedSHA: expectedSHA,
                            observedSHA: nil
                        ),
                        evidence: nil
                    )
                }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) {
                    return .inconclusive(reason: .cancelled)
                }
                return .inconclusive(reason: .retainedBarrierCheckpointReadFailed(filename: filename))
            }
            guard snapshotFile.sha256Hex.lowercased() == expectedSHA.lowercased() else {
                return .failed(
                    reason: .retainedBarrierCheckpointMissingOrTampered(
                        filename: filename,
                        expectedSHA: expectedSHA,
                        observedSHA: snapshotFile.sha256Hex.lowercased()
                    ),
                    evidence: nil
                )
            }
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
