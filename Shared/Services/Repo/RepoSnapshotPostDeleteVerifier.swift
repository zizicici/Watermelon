import Foundation

enum RepoSnapshotPostDeleteVerificationResult: Equatable, Sendable {
    case passed(evidence: RepoSnapshotPostDeleteVerificationEvidence)
    case failed(reason: RepoSnapshotPostDeleteVerificationFailure, evidence: RepoSnapshotPostDeleteVerificationEvidence?)
    case inconclusive(reason: RepoSnapshotPostDeleteVerificationInconclusive)
}

struct RepoSnapshotPostDeleteVerificationEvidence: Equatable, Sendable {
    let acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo
    let materializedCovered: CoveredRanges
    let observedSeqByWriter: [String: UInt64]
    let observedClock: UInt64
    let validatedProtectedSnapshots: [String]
}

enum RepoSnapshotPostDeleteVerificationFailure: Equatable, Sendable {
    case missingRepoIdentity(expected: String)
    case repoIdentityMismatch(expected: String, observed: String)
    case missingAcceptedSnapshot(month: LibraryMonthKey)
    case acceptedSnapshotCoverageRegression(filename: String)
    case retainedBarrierCoverageRegression(filename: String)
    case stateNotRetentionSuperset
    case coveredRangeRegression
    case observedSeqRegression(writerID: String, expectedAtLeast: UInt64, observed: UInt64)
    case observedClockRegression(expectedAtLeast: UInt64, observed: UInt64)
    case acceptedSnapshotSupersedeUnsafe(expectedFilename: String, observedFilename: String, observedLamport: UInt64)
    case acceptedSnapshotContentMismatch(filename: String, expectedSHA: String, observedSHA: String)
    case protectedSnapshotMissingOrTampered(filename: String, expectedSHA: String, observedSHA: String?)
}

enum RepoSnapshotPostDeleteVerificationInconclusive: Equatable, Sendable {
    case repoIdentityReadFailed
    case materializerReadRace
    case materializerReadFailed
    case protectedSnapshotReadFailed(filename: String)
    case deleteTargetStillPresent(path: String)
    case cancelled
}

struct RepoSnapshotPostDeleteEquivalenceContract: Equatable, Sendable {
    let acceptedSnapshotFilename: String
    let acceptedSnapshotLamport: UInt64
    let acceptedSnapshotSHA256Hex: String
    let acceptedSnapshotCovered: CoveredRanges
    let retainedBarrierUnionCovered: CoveredRanges
    let retainedManifestCheckpointSHA256ByFilename: [String: String]
    // Protection-set snapshots that are neither the accepted baseline nor a barrier checkpoint
    // (e.g. fallback-protected by snapshotKeepCount). Verifier checks these too so a storage
    // fault that deletes a non-target protected snapshot during GC fails closed.
    let additionalProtectedSnapshotSHA256ByFilename: [String: String]
    let requiredObservedSeqByWriter: [String: UInt64]
    let preDeleteCovered: CoveredRanges
    let preDeleteState: RepoSnapshotState
    let preDeleteObservedClock: UInt64
}

struct RepoSnapshotPostDeleteVerifier: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = wrapIfSerial(client)
        self.basePath = basePath
    }

    func verify(
        month: LibraryMonthKey,
        expectedRepoID: String,
        contract: RepoSnapshotPostDeleteEquivalenceContract
    ) async -> RepoSnapshotPostDeleteVerificationResult {
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

        // Validate protected snapshots still parse and hash-match before signalling pass.
        let snapshotReader = SnapshotReader(client: client, basePath: basePath)
        var validatedFilenames: [String] = []
        // Merge barrier-checkpoint and additional fallback-protected SHAs. Same filename in
        // both must have the same SHA in practice; keep the barrier SHA on collision for
        // diagnostic clarity.
        var protectedSHAByFilename = contract.additionalProtectedSnapshotSHA256ByFilename
        for (filename, sha) in contract.retainedManifestCheckpointSHA256ByFilename {
            protectedSHAByFilename[filename] = sha
        }
        for (filename, expectedSHA) in protectedSHAByFilename
            .sorted(by: { $0.key < $1.key }) {
            let snapshotFile: SnapshotFile
            do {
                snapshotFile = try await snapshotReader.read(filename: filename)
            } catch let error as RepoJSONLReadError {
                switch error {
                case .notFound:
                    return .failed(
                        reason: .protectedSnapshotMissingOrTampered(
                            filename: filename,
                            expectedSHA: expectedSHA,
                            observedSHA: nil
                        ),
                        evidence: nil
                    )
                case .missingHeader, .missingEnd, .integrityMismatch, .decodeFailure:
                    return .failed(
                        reason: .protectedSnapshotMissingOrTampered(
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
                return .inconclusive(reason: .protectedSnapshotReadFailed(filename: filename))
            }
            guard snapshotFile.sha256Hex.lowercased() == expectedSHA.lowercased() else {
                return .failed(
                    reason: .protectedSnapshotMissingOrTampered(
                        filename: filename,
                        expectedSHA: expectedSHA,
                        observedSHA: snapshotFile.sha256Hex.lowercased()
                    ),
                    evidence: nil
                )
            }
            validatedFilenames.append(filename)
        }

        // Always re-read the pre-delete accepted baseline. It's in protectedFilenames but
        // excluded from `additionalProtectedSnapshotSHA256ByFilename` (carried as its own
        // field). When a newer post-delete baseline supersedes it, skipping this check
        // would let the pre-delete baseline silently disappear during the delete window.
        do {
            let file = try await snapshotReader.read(filename: contract.acceptedSnapshotFilename)
            guard file.sha256Hex.lowercased() == contract.acceptedSnapshotSHA256Hex.lowercased() else {
                if acceptedSnapshot.filename == contract.acceptedSnapshotFilename {
                    return .failed(
                        reason: .acceptedSnapshotContentMismatch(
                            filename: contract.acceptedSnapshotFilename,
                            expectedSHA: contract.acceptedSnapshotSHA256Hex.lowercased(),
                            observedSHA: file.sha256Hex.lowercased()
                        ),
                        evidence: nil
                    )
                }
                return .failed(
                    reason: .protectedSnapshotMissingOrTampered(
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
                    reason: .protectedSnapshotMissingOrTampered(
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
            return .inconclusive(reason: .protectedSnapshotReadFailed(filename: contract.acceptedSnapshotFilename))
        }

        let evidence = RepoSnapshotPostDeleteVerificationEvidence(
            acceptedSnapshot: acceptedSnapshot,
            materializedCovered: output.coveredByMonth[month, default: .empty],
            observedSeqByWriter: output.observedSeqByWriter,
            observedClock: output.state.observedClock,
            validatedProtectedSnapshots: validatedFilenames
        )
        switch RetentionInvariantEvaluator.evaluateSnapshotPostDeleteContract(
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
