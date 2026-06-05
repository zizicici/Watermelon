import Foundation

struct RepoSnapshotPostDeleteLightweightVerifier: Sendable {
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

        let snapshotReader = SnapshotReader(client: client, basePath: basePath)

        var validatedProtectedFilenames: [String] = []
        for (filename, expectedSHA) in contract.additionalProtectedSnapshotSHA256ByFilename
            .sorted(by: { $0.key < $1.key }) {
            let file: SnapshotFile
            do {
                file = try await snapshotReader.read(filename: filename)
            } catch let error as RepoJSONLReadError {
                switch error {
                case .notFound, .missingHeader, .missingEnd, .integrityMismatch, .decodeFailure:
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
            guard file.sha256Hex.lowercased() == expectedSHA.lowercased() else {
                return .failed(
                    reason: .protectedSnapshotMissingOrTampered(
                        filename: filename,
                        expectedSHA: expectedSHA,
                        observedSHA: file.sha256Hex.lowercased()
                    ),
                    evidence: nil
                )
            }
            validatedProtectedFilenames.append(filename)
        }

        let acceptedFile: SnapshotFile
        do {
            acceptedFile = try await snapshotReader.read(filename: contract.acceptedSnapshotFilename)
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

        guard acceptedFile.sha256Hex.lowercased() == contract.acceptedSnapshotSHA256Hex.lowercased() else {
            return .failed(
                reason: .acceptedSnapshotContentMismatch(
                    filename: contract.acceptedSnapshotFilename,
                    expectedSHA: contract.acceptedSnapshotSHA256Hex.lowercased(),
                    observedSHA: acceptedFile.sha256Hex.lowercased()
                ),
                evidence: nil
            )
        }

        let acceptedCovered = acceptedFile.header.covered
        guard acceptedCovered.superset(of: contract.acceptedSnapshotCovered) else {
            return .failed(
                reason: .acceptedSnapshotCoverageRegression(filename: contract.acceptedSnapshotFilename),
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
        let evidence = RepoSnapshotPostDeleteVerificationEvidence(
            acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo(
                filename: contract.acceptedSnapshotFilename,
                month: month,
                lamport: parsed?.lamport ?? contract.acceptedSnapshotLamport,
                writerID: acceptedFile.header.writerID,
                runIDPrefix: parsed?.runIDPrefix ?? "",
                covered: acceptedCovered
            ),
            materializedCovered: contract.preDeleteCovered,
            observedSeqByWriter: contract.requiredObservedSeqByWriter,
            observedClock: contract.preDeleteObservedClock,
            validatedProtectedSnapshots: validatedProtectedFilenames
        )
        return .passed(evidence: evidence)
    }

}

enum RepoSnapshotCoveredMaxAuthorityCheckResult: Sendable {
    case confirmed
    case materializerReadFailed
    case cancelled
}

enum RepoSnapshotCoveredMaxAuthorityChecker {
    static func verify(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        snapshotReader: SnapshotReader,
        acceptedCovered: CoveredRanges,
        acceptedFilename: String,
        repoID: String,
        month: LibraryMonthKey
    ) async -> RepoSnapshotCoveredMaxAuthorityCheckResult {
        let dir = RepoLayout.snapshotsDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) {
                return .cancelled
            }
            return .materializerReadFailed
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
                    return .materializerReadFailed
                case .missingHeader, .missingEnd, .integrityMismatch, .decodeFailure:
                    continue
                }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) {
                    return .cancelled
                }
                return .materializerReadFailed
            }

            guard RepoCanonicalIdentity.normalizeLossy(candidateFile.header.repoID) == repoID else {
                continue
            }
            guard candidateFile.header.writerID == parsed.writerID,
                  CommitHeader.parseMonthScope(candidateFile.header.scope) == month else {
                continue
            }
            guard SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(
                candidateFile,
                month: parsed.month,
                filenameLamport: parsed.lamport
            ) else {
                continue
            }

            guard acceptedCovered.superset(of: candidateFile.header.covered) else {
                return .materializerReadFailed
            }
        }

        guard acceptedListed else {
            return .materializerReadFailed
        }
        return .confirmed
    }
}
