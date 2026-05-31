import Foundation

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

        let acceptedStillAuthority = await verifyAcceptedStillCoveredMaxAuthority(
            snapshotReader: snapshotReader,
            acceptedCovered: acceptedCovered,
            acceptedFilename: contract.acceptedSnapshotFilename,
            repoID: repoID,
            month: month
        )
        switch acceptedStillAuthority {
        case .confirmed:
            break
        case .inconclusive(let reason):
            return .inconclusive(reason: reason)
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

    private enum CoveredMaxCheckResult: Sendable {
        case confirmed
        case inconclusive(reason: RepoRetentionPostDeleteVerificationInconclusive)
    }

    private func verifyAcceptedStillCoveredMaxAuthority(
        snapshotReader: SnapshotReader,
        acceptedCovered: CoveredRanges,
        acceptedFilename: String,
        repoID: String,
        month: LibraryMonthKey
    ) async -> CoveredMaxCheckResult {
        let dir = RepoLayout.snapshotsDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) {
                return .inconclusive(reason: .cancelled)
            }
            return .inconclusive(reason: .materializerReadFailed)
        }

        var acceptedListed = false
        for entry in entries.sorted(by: { $0.name < $1.name }) {
            guard !entry.isDirectory, entry.name.hasSuffix(".jsonl") else { continue }
            guard let parsed = RepoLayout.parseSnapshotFilename(entry.name),
                  parsed.month == month else { continue }

            if entry.name == acceptedFilename {
                acceptedListed = true
                continue
            }

            let candidateFile: SnapshotFile
            do {
                candidateFile = try await snapshotReader.read(filename: entry.name)
            } catch let error as RepoJSONLReadError {
                switch error {
                case .notFound:
                    continue
                case .missingHeader, .missingEnd, .integrityMismatch, .decodeFailure:
                    return .inconclusive(reason: .materializerReadFailed)
                }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) {
                    return .inconclusive(reason: .cancelled)
                }
                return .inconclusive(reason: .materializerReadFailed)
            }

            guard RepoCanonicalIdentity.normalizeLossy(candidateFile.header.repoID) == repoID else {
                continue
            }

            guard acceptedCovered.superset(of: candidateFile.header.covered) else {
                return .inconclusive(reason: .materializerReadFailed)
            }
        }

        guard acceptedListed else {
            return .inconclusive(reason: .materializerReadFailed)
        }
        return .confirmed
    }
}
