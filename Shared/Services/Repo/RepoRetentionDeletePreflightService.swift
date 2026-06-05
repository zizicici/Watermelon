import Foundation

enum RepoRetentionDeletePreflightMode: Equatable, Sendable {
    case dryRun
}

enum RepoRetentionCandidateHeaderMismatchReason: Equatable, Sendable {
    case repoID(expected: String, actual: String)
    case month(expected: LibraryMonthKey, actual: LibraryMonthKey?)
    case writerID(expected: String, actual: String)
    case seq(expected: UInt64, actual: UInt64)
}

enum RepoRetentionDeletePreflightBlocker: Equatable, Sendable {
    case missingVersion
    case unreadableVersion
    case unsupportedVersion(formatVersion: Int)
    case repoIdentityMismatch(expected: String, observed: String)
    case migrationInProgress
    case migrationCheckFailed
    case migrationResiduePresent(month: LibraryMonthKey)
    case migrationResidueCheckFailed(month: LibraryMonthKey)
    case materializerReadRace
    case materializerReadFailed
    case noAcceptedSnapshot(month: LibraryMonthKey)
    case noDeleteCandidates
    case candidateListFailed
    case candidateReadFailed(filename: String)
    case candidateHeaderMismatch(filename: String, reason: RepoRetentionCandidateHeaderMismatchReason)
    case candidateCorruptOrUntrusted(filename: String)
    case plannerReadFailed
    case plannerCrossCheckFailed(plannedCount: Int, plannerCount: Int, plannedBytes: Int64, plannerBytes: Int64)
}

struct RepoRetentionProtectedSummary: Equatable, Sendable {
    var targetMonthUnparseableFilenameCount: Int = 0
    var crossMonthCommitFileCount: Int = 0
    var outOfPrefixCommitFileCount: Int = 0
    var ignoredNonCommitEntryCount: Int = 0
    var headerMismatchCandidateCount: Int = 0
    var corruptOrUntrustedCandidateCount: Int = 0
    var readFailedCandidateCount: Int = 0
    var protectedBytes: Int64 = 0
}

struct RepoRetentionDeleteCandidateScanResult: Equatable, Sendable {
    let candidates: [RepoMetadataDeleteCandidate]
    let protectedSummary: RepoRetentionProtectedSummary
    let blockers: [RepoRetentionDeletePreflightBlocker]
    let readConcurrencyLimit: Int
}

enum RepoRetentionPostDeleteEquivalenceMode: Equatable, Sendable {
    case retentionSuperset
}

struct RepoRetentionPostDeleteEquivalenceContract: Equatable, Sendable {
    let mode: RepoRetentionPostDeleteEquivalenceMode
    let acceptedSnapshotFilename: String
    let acceptedSnapshotSHA256Hex: String
    let acceptedSnapshotCovered: CoveredRanges
    let requiredObservedSeqByWriter: [String: UInt64]
    let expectedDeletePrefixByWriter: [String: UInt64]
    let preDeleteCovered: CoveredRanges
    let preDeleteState: RepoSnapshotState
}

struct RepoRetentionPreDeleteEvidence: Equatable, Sendable {
    let materializedState: RepoSnapshotState
    let materializedCovered: CoveredRanges
    let observedSeqByWriter: [String: UInt64]
    let acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo
    let postDeleteEquivalenceContract: RepoRetentionPostDeleteEquivalenceContract
}

struct RepoRetentionDeletePreflightPlan: Equatable, Sendable {
    let month: LibraryMonthKey
    let repoID: String
    let acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo
    let deletePrefixByWriter: [String: UInt64]
    let commitFiles: [RepoMetadataDeleteCandidate]
    let protectedSummary: RepoRetentionProtectedSummary
    let preDeleteEvidence: RepoRetentionPreDeleteEvidence
}

struct RepoRetentionDeletePreflightReport: Equatable, Sendable {
    let month: LibraryMonthKey
    let repoID: String
    let mode: RepoRetentionDeletePreflightMode
    let evaluatedAtMs: Int64
    var acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo?
    var materializedCovered: CoveredRanges?
    var observedSeqByWriter: [String: UInt64] = [:]
    var deletePrefixByWriter: [String: UInt64] = [:]
    var candidateScan: RepoRetentionDeleteCandidateScanResult?
    var compactionMonthReport: RepoCompactionMonthReport?
}

struct RepoRetentionDeleteCandidateScanner: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    func scan(
        month: LibraryMonthKey,
        expectedRepoID: String,
        deletePrefixByWriter: [String: UInt64]
    ) async throws -> RepoRetentionDeleteCandidateScanResult {
        let dir = RepoLayout.commitsDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return RepoRetentionDeleteCandidateScanResult(
                candidates: [],
                protectedSummary: RepoRetentionProtectedSummary(),
                blockers: [.candidateListFailed],
                readConcurrencyLimit: 1
            )
        }

        let reader = CommitLogReader(client: client, basePath: basePath)
        var candidates: [RepoMetadataDeleteCandidate] = []
        var protectedSummary = RepoRetentionProtectedSummary()
        var blockers: [RepoRetentionDeletePreflightBlocker] = []

        for entry in entries.sorted(by: { $0.name < $1.name }) {
            if entry.isDirectory {
                if entry.name.hasSuffix(".jsonl"),
                   let parsed = RepoLayout.parseCommitFilename(entry.name),
                   parsed.month == month,
                   parsed.seq > 0,
                   let prefix = deletePrefixByWriter[parsed.writerID],
                   parsed.seq <= prefix {
                    protectedSummary.corruptOrUntrustedCandidateCount += 1
                    blockers.append(.candidateCorruptOrUntrusted(filename: entry.name))
                } else {
                    protectedSummary.ignoredNonCommitEntryCount += 1
                }
                continue
            }
            guard entry.name.hasSuffix(".jsonl") else {
                protectedSummary.ignoredNonCommitEntryCount += 1
                continue
            }
            guard let parsed = RepoLayout.parseCommitFilename(entry.name) else {
                if monthPrefix(from: entry.name) == month {
                    protectedSummary.targetMonthUnparseableFilenameCount += 1
                    protectedSummary.protectedBytes += entry.size
                } else {
                    protectedSummary.ignoredNonCommitEntryCount += 1
                }
                continue
            }
            guard parsed.month == month else {
                protectedSummary.crossMonthCommitFileCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }
            guard parsed.seq > 0 else {
                protectedSummary.outOfPrefixCommitFileCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }
            guard entry.name == RepoLayout.commitFileName(
                month: parsed.month,
                writerID: parsed.writerID,
                seq: parsed.seq
            ) else {
                protectedSummary.targetMonthUnparseableFilenameCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }
            guard let prefix = deletePrefixByWriter[parsed.writerID], parsed.seq <= prefix else {
                protectedSummary.outOfPrefixCommitFileCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }
            let listedPath = RemotePathBuilder.absolutePath(basePath: dir, remoteRelativePath: entry.name)

            let commit: CommitFile
            do {
                commit = try await reader.read(remotePath: listedPath)
            } catch let error as RepoJSONLReadError {
                switch error {
                case .notFound:
                    protectedSummary.readFailedCandidateCount += 1
                    protectedSummary.protectedBytes += entry.size
                    blockers.append(.candidateReadFailed(filename: entry.name))
                case .missingHeader, .missingEnd, .integrityMismatch(_), .decodeFailure(_):
                    protectedSummary.corruptOrUntrustedCandidateCount += 1
                    protectedSummary.protectedBytes += entry.size
                    blockers.append(.candidateCorruptOrUntrusted(filename: entry.name))
                }
                continue
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                protectedSummary.readFailedCandidateCount += 1
                protectedSummary.protectedBytes += entry.size
                blockers.append(.candidateReadFailed(filename: entry.name))
                continue
            }

            if let mismatch = headerMismatch(
                parsed: parsed,
                header: commit.header,
                expectedRepoID: expectedRepoID
            ) {
                protectedSummary.headerMismatchCandidateCount += 1
                protectedSummary.protectedBytes += entry.size
                blockers.append(.candidateHeaderMismatch(filename: entry.name, reason: mismatch))
                continue
            }

            if commit.ops.contains(where: { $0.clock >= LamportClock.maxAdoptableValue }) {
                protectedSummary.corruptOrUntrustedCandidateCount += 1
                protectedSummary.protectedBytes += entry.size
                blockers.append(.candidateCorruptOrUntrusted(filename: entry.name))
                continue
            }

            candidates.append(RepoMetadataDeleteCandidate(
                kind: .commit(seq: parsed.seq),
                filename: entry.name,
                path: listedPath,
                month: parsed.month,
                writerID: parsed.writerID,
                size: entry.size,
                sha256Hex: commit.sha256Hex.lowercased(),
                rowCount: commit.rowCount
            ))
        }

        candidates.sort {
            if $0.writerID != $1.writerID { return $0.writerID < $1.writerID }
            return ($0.commitSeq ?? 0) < ($1.commitSeq ?? 0)
        }
        return RepoRetentionDeleteCandidateScanResult(
            candidates: candidates,
            protectedSummary: protectedSummary,
            blockers: blockers,
            readConcurrencyLimit: 1
        )
    }

    private func headerMismatch(
        parsed: RepoLayout.ParsedCommitFilename,
        header: CommitHeader,
        expectedRepoID: String
    ) -> RepoRetentionCandidateHeaderMismatchReason? {
        if RepoCanonicalIdentity.normalizeLossy(header.repoID) != expectedRepoID {
            return .repoID(expected: expectedRepoID, actual: RepoCanonicalIdentity.normalizeLossy(header.repoID))
        }
        if header.writerID != parsed.writerID {
            return .writerID(expected: parsed.writerID, actual: header.writerID)
        }
        if header.seq != parsed.seq {
            return .seq(expected: parsed.seq, actual: header.seq)
        }
        let scopeMonth = CommitHeader.parseMonthScope(header.scope)
        if scopeMonth != parsed.month {
            return .month(expected: parsed.month, actual: scopeMonth)
        }
        return nil
    }
}

private func monthPrefix(from filename: String) -> LibraryMonthKey? {
    guard filename.count >= 7 else { return nil }
    let prefix = String(filename.prefix(7))
    let parts = prefix.split(separator: "-")
    guard parts.count == 2,
          let year = Int(parts[0]),
          let month = Int(parts[1]),
          (1...12).contains(month) else {
        return nil
    }
    return LibraryMonthKey(year: year, month: month)
}
