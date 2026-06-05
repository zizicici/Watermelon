import Foundation

enum RepoSnapshotCandidateHeaderMismatchReason: Equatable, Sendable {
    case repoID(expected: String, actual: String)
    case month(expected: LibraryMonthKey, actual: LibraryMonthKey?)
    case writerID(expected: String, actual: String)
}

enum RepoSnapshotDeletePreflightBlocker: Equatable, Sendable {
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
    case noAcceptedPerMonthSnapshot(month: LibraryMonthKey)
    case noDeleteCandidates
    case snapshotListFailed
    case candidateReadFailed(filename: String)
    case candidateCorruptOrUntrusted(filename: String)
    case candidateHeaderMismatch(filename: String, reason: RepoSnapshotCandidateHeaderMismatchReason)
    case acceptedBaselineNotListed(filename: String)
}

struct RepoSnapshotDeleteCandidate: Equatable, Sendable {
    let filename: String
    let path: String
    let month: LibraryMonthKey
    let writerID: String
    let lamport: UInt64
    let runIDPrefix: String
    let size: Int64
    let sha256Hex: String
    let rowCount: Int
}

struct RepoSnapshotProtectedSummary: Equatable, Sendable {
    var unparseableSnapshotsForMonth: Int = 0
    var crossMonthSnapshotCount: Int = 0
    var ignoredNonSnapshotEntryCount: Int = 0
    var headerMismatchCandidateCount: Int = 0
    var corruptOrUntrustedCandidateCount: Int = 0
    var readFailedCandidateCount: Int = 0
    var protectedBytes: Int64 = 0
}

struct RepoSnapshotDeleteCandidateScanResult: Equatable, Sendable {
    let parseableSnapshots: [Parseable]
    let candidates: [RepoSnapshotDeleteCandidate]
    let protectedSummary: RepoSnapshotProtectedSummary
    let blockers: [RepoSnapshotDeletePreflightBlocker]
    let acceptedBaselineListed: Bool

    struct Parseable: Equatable, Sendable {
        let filename: String
        let lamport: UInt64
        let writerID: String
        let covered: CoveredRanges
    }
}

struct RepoSnapshotDeletePreflightPlan: Equatable, Sendable {
    let month: LibraryMonthKey
    let repoID: String
    let acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo
    let acceptedSnapshotSHA256Hex: String
    let protectedFilenames: Set<String>
    let snapshotsToDelete: [RepoSnapshotDeleteCandidate]
    let protectedSummary: RepoSnapshotProtectedSummary
    let postDeleteContract: RepoSnapshotPostDeleteEquivalenceContract
}

struct RepoSnapshotDeletePreflightReport: Equatable, Sendable {
    let month: LibraryMonthKey
    let repoID: String
    let evaluatedAtMs: Int64
    var acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo?
    var materializedCovered: CoveredRanges?
    var observedSeqByWriter: [String: UInt64] = [:]
    var protectedFilenames: Set<String> = []
    var candidateScan: RepoSnapshotDeleteCandidateScanResult?
}

struct SnapshotDeleteCandidateScanner: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String
    let policy: RepoCompactionPolicy

    func scan(
        month: LibraryMonthKey,
        expectedRepoID: String,
        acceptedBaseline: RepoMaterializer.AcceptedSnapshotBaselineInfo
    ) async throws -> RepoSnapshotDeleteCandidateScanResult {
        let dir = RepoLayout.snapshotsDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            if isStorageNotFoundError(error) {
                return RepoSnapshotDeleteCandidateScanResult(
                    parseableSnapshots: [],
                    candidates: [],
                    protectedSummary: RepoSnapshotProtectedSummary(),
                    blockers: [.snapshotListFailed],
                    acceptedBaselineListed: false
                )
            }
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return RepoSnapshotDeleteCandidateScanResult(
                parseableSnapshots: [],
                candidates: [],
                protectedSummary: RepoSnapshotProtectedSummary(),
                blockers: [.snapshotListFailed],
                acceptedBaselineListed: false
            )
        }

        let reader = SnapshotReader(client: client, basePath: basePath)
        var parseable: [RepoSnapshotDeleteCandidateScanResult.Parseable] = []
        var candidates: [RepoSnapshotDeleteCandidate] = []
        var protectedSummary = RepoSnapshotProtectedSummary()
        var blockers: [RepoSnapshotDeletePreflightBlocker] = []
        var acceptedBaselineListed = false

        for entry in entries.sorted(by: { $0.name < $1.name }) {
            if entry.isDirectory {
                if entry.name.hasSuffix(".jsonl"),
                   let monthHint = monthPrefix(from: entry.name), monthHint == month {
                    protectedSummary.unparseableSnapshotsForMonth += 1
                } else {
                    protectedSummary.ignoredNonSnapshotEntryCount += 1
                }
                continue
            }
            guard entry.name.hasSuffix(".jsonl") else {
                protectedSummary.ignoredNonSnapshotEntryCount += 1
                continue
            }
            guard let parsed = RepoLayout.parseSnapshotFilename(entry.name) else {
                if let monthHint = monthPrefix(from: entry.name), monthHint == month {
                    protectedSummary.unparseableSnapshotsForMonth += 1
                    protectedSummary.protectedBytes += entry.size
                } else {
                    protectedSummary.ignoredNonSnapshotEntryCount += 1
                }
                continue
            }
            guard parsed.month == month else {
                protectedSummary.crossMonthSnapshotCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }

            let snapshotFile: SnapshotFile
            do {
                snapshotFile = try await reader.read(filename: entry.name)
            } catch let error as RepoJSONLReadError {
                switch error {
                case .notFound:
                    protectedSummary.readFailedCandidateCount += 1
                    protectedSummary.protectedBytes += entry.size
                    blockers.append(.candidateReadFailed(filename: entry.name))
                case .missingHeader, .missingEnd, .integrityMismatch, .decodeFailure:
                    protectedSummary.corruptOrUntrustedCandidateCount += 1
                    protectedSummary.protectedBytes += entry.size
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
                header: snapshotFile.header,
                expectedRepoID: expectedRepoID
            ) {
                protectedSummary.headerMismatchCandidateCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }

            if !snapshotBodyIsMaterializerTrusted(snapshotFile, month: parsed.month, filenameLamport: parsed.lamport) {
                protectedSummary.corruptOrUntrustedCandidateCount += 1
                protectedSummary.protectedBytes += entry.size
                continue
            }

            parseable.append(.init(
                filename: entry.name,
                lamport: parsed.lamport,
                writerID: parsed.writerID,
                covered: snapshotFile.header.covered
            ))

            if entry.name == acceptedBaseline.filename {
                acceptedBaselineListed = true
            }

            // List every validated snapshot; deletability (domination + keepN) is decided downstream.
            candidates.append(RepoSnapshotDeleteCandidate(
                filename: entry.name,
                path: RemotePathBuilder.absolutePath(basePath: dir, remoteRelativePath: entry.name),
                month: parsed.month,
                writerID: parsed.writerID,
                lamport: parsed.lamport,
                runIDPrefix: parsed.runIDPrefix,
                size: entry.size,
                sha256Hex: snapshotFile.sha256Hex.lowercased(),
                rowCount: snapshotFile.rowCount
            ))
        }

        candidates.sort { lhs, rhs in
            if lhs.lamport != rhs.lamport { return lhs.lamport < rhs.lamport }
            return lhs.filename < rhs.filename
        }

        return RepoSnapshotDeleteCandidateScanResult(
            parseableSnapshots: parseable,
            candidates: candidates,
            protectedSummary: protectedSummary,
            blockers: blockers,
            acceptedBaselineListed: acceptedBaselineListed
        )
    }

    private func headerMismatch(
        parsed: RepoLayout.ParsedSnapshotFilename,
        header: SnapshotHeader,
        expectedRepoID: String
    ) -> RepoSnapshotCandidateHeaderMismatchReason? {
        if RepoCanonicalIdentity.normalizeLossy(header.repoID) != expectedRepoID {
            return .repoID(
                expected: expectedRepoID,
                actual: RepoCanonicalIdentity.normalizeLossy(header.repoID)
            )
        }
        let scopeMonth = CommitHeader.parseMonthScope(header.scope)
        if scopeMonth != parsed.month {
            return .month(expected: parsed.month, actual: scopeMonth)
        }
        if header.writerID != parsed.writerID {
            return .writerID(expected: parsed.writerID, actual: header.writerID)
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

func snapshotBodyIsMaterializerTrusted(
    _ file: SnapshotFile,
    month: LibraryMonthKey,
    filenameLamport: UInt64
) -> Bool {
    guard filenameLamport < LamportClock.maxAdoptableValue else { return false }
    var assets: Set<AssetFingerprint> = []
    for asset in file.assets {
        assets.insert(asset.assetFingerprint)
    }
    var resourcePaths: Set<RemotePhysicalPathKey> = []
    var resourceHashes: Set<Data> = []
    for resource in file.resources {
        if !snapshotResourcePathBelongsTo(resource.physicalRemotePath, month: month) {
            return false
        }
        let key = RemotePhysicalPathKey(resource.physicalRemotePath)
        if resourcePaths.contains(key) { return false }
        resourcePaths.insert(key)
        resourceHashes.insert(resource.contentHash)
    }
    let covered = file.header.covered
    for asset in file.assets {
        if !rowStampIsWorkable(asset.stamp, covered: covered, filenameLamport: filenameLamport) {
            return false
        }
    }
    for resource in file.resources {
        if !rowStampIsWorkable(resource.stamp, covered: covered, filenameLamport: filenameLamport) {
            return false
        }
    }
    for deletedKey in file.deletedKeys {
        if !rowStampIsWorkable(deletedKey.stamp, covered: covered, filenameLamport: filenameLamport) {
            return false
        }
        guard deletedKey.keyType == .asset else { return false }
        do {
            _ = try RepoWireValidator.validateHash(deletedKey.keyValue, field: "keyValue")
        } catch {
            return false
        }
    }
    var linkedAssets: Set<AssetFingerprint> = []
    for link in file.assetResources {
        guard assets.contains(link.assetFingerprint) else { return false }
        guard resourceHashes.contains(link.resourceHash) else { return false }
        linkedAssets.insert(link.assetFingerprint)
    }
    for asset in file.assets where !linkedAssets.contains(asset.assetFingerprint) {
        return false
    }
    for deletedKey in file.deletedKeys where deletedKey.keyType == .asset {
        guard let fp = try? RepoWireValidator.validateAssetFingerprint(deletedKey.keyValue, field: "keyValue") else {
            return false
        }
        if assets.contains(fp) { return false }
    }
    return true
}

private func snapshotResourcePathBelongsTo(_ path: String, month: LibraryMonthKey) -> Bool {
    let components = RemotePathBuilder.normalizeRelativePath(path)
        .split(separator: "/", omittingEmptySubsequences: false)
    guard components.count == 3, !components[2].isEmpty else { return false }
    let expectedYear = String(format: "%04d", month.year)
    let expectedMonth = String(format: "%02d", month.month)
    return String(components[0]) == expectedYear && String(components[1]) == expectedMonth
}

private func rowStampIsWorkable(
    _ stamp: OpStamp,
    covered: CoveredRanges,
    filenameLamport: UInt64
) -> Bool {
    if stamp.clock >= LamportClock.maxAdoptableValue { return false }
    if stamp.clock > filenameLamport { return false }
    return covered.contains(writerID: stamp.writerID, seq: stamp.seq)
}
