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
}

enum RepoRetentionPostDeleteVerificationInconclusive: Equatable, Sendable {
    case repoIdentityReadFailed
    case materializerReadRace
    case materializerReadFailed
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
        let repoID = canonicalRepoIDForRetentionDelete(expectedRepoID)
        do {
            switch try await RepoBootstrap(client: client, basePath: basePath).loadRepoIDStrict() {
            case .absent:
                return .failed(reason: .missingRepoIdentity(expected: repoID), evidence: nil)
            case .found(let observed):
                let observedRepoID = canonicalRepoIDForRetentionDelete(observed)
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
        let evidence = RepoRetentionPostDeleteVerificationEvidence(
            acceptedSnapshot: acceptedSnapshot,
            materializedCovered: output.coveredByMonth[month, default: .empty],
            observedSeqByWriter: output.observedSeqByWriter,
            observedClock: output.state.observedClock
        )

        guard acceptedSnapshot.covered.superset(of: contract.acceptedSnapshotCovered) else {
            return .failed(
                reason: .acceptedSnapshotCoverageRegression(filename: acceptedSnapshot.filename),
                evidence: evidence
            )
        }
        guard acceptedSnapshot.covered.superset(of: contract.retainedBarrierUnionCovered) else {
            return .failed(
                reason: .retainedBarrierCoverageRegression(filename: acceptedSnapshot.filename),
                evidence: evidence
            )
        }
        guard acceptedSnapshot.covered.superset(of: Self.coveredRanges(fromPrefixes: contract.expectedDeletePrefixByWriter)) else {
            return .failed(
                reason: .deletePrefixCoverageRegression(filename: acceptedSnapshot.filename),
                evidence: evidence
            )
        }
        guard Self.stateIsRetentionSuperset(before: contract.preDeleteState, after: output.state, month: month) else {
            return .failed(reason: .stateNotRetentionSuperset, evidence: evidence)
        }
        guard evidence.materializedCovered.superset(of: contract.preDeleteCovered) else {
            return .failed(reason: .coveredRangeRegression, evidence: evidence)
        }
        for writerID in contract.requiredObservedSeqByWriter.keys.sorted() {
            let expected = contract.requiredObservedSeqByWriter[writerID] ?? 0
            let observed = evidence.observedSeqByWriter[writerID] ?? 0
            guard observed >= expected else {
                return .failed(
                    reason: .observedSeqRegression(
                        writerID: writerID,
                        expectedAtLeast: expected,
                        observed: observed
                    ),
                    evidence: evidence
                )
            }
        }
        guard output.state.observedClock >= contract.preDeleteState.observedClock else {
            return .failed(
                reason: .observedClockRegression(
                    expectedAtLeast: contract.preDeleteState.observedClock,
                    observed: output.state.observedClock
                ),
                evidence: evidence
            )
        }
        return .passed(evidence: evidence)
    }

    private static func coveredRanges(fromPrefixes prefixes: [String: UInt64]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: prefixes.compactMapValues { prefix in
            prefix > 0 ? [ClosedSeqRange(low: 1, high: prefix)] : nil
        })
    }

    private static func stateIsRetentionSuperset(
        before: RepoSnapshotState,
        after: RepoSnapshotState,
        month: LibraryMonthKey
    ) -> Bool {
        guard let beforeMonth = before.months[month] else { return true }
        guard let afterMonth = after.months[month] else { return false }
        return monthStateIsSuperset(before: beforeMonth, after: afterMonth)
    }

    private static func monthStateIsSuperset(before: RepoMonthState, after: RepoMonthState) -> Bool {
        dictionaryIsSuperset(before: before.assets, after: after.assets) &&
        dictionaryIsSuperset(before: before.resources, after: after.resources) &&
        dictionaryIsSuperset(before: before.assetResources, after: after.assetResources) &&
        after.deletedAssetFingerprints.isSuperset(of: before.deletedAssetFingerprints) &&
        dictionaryIsSuperset(before: before.deletedAssetStamps, after: after.deletedAssetStamps)
    }

    private static func dictionaryIsSuperset<K: Hashable, V: Equatable>(before: [K: V], after: [K: V]) -> Bool {
        for (key, value) in before where after[key] != value {
            return false
        }
        return true
    }
}

func canonicalRepoIDForRetentionDelete(_ value: String) -> String {
    UUID(uuidString: value)?.uuidString.lowercased() ?? value.lowercased()
}
