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
    case invalidBarrierSet([InvalidRetentionManifestEntry])
    case barrierSetReadFailed
    case emptyBarrierSet
    case barrierTooYoung(filename: String, createdAtMs: Int64)
    case barrierCreatedInFuture(filename: String, createdAtMs: Int64)
    case barrierCheckpointReadFailed(filename: String)
    case barrierCheckpointMismatch(filename: String, reason: RepoRetentionBarrierCheckpointMismatchReason)
    case barrierObservedSeqRegression(writerID: String, expectedAtLeast: UInt64, observed: UInt64)
    case materializerReadRace
    case materializerReadFailed
    case noAcceptedPerMonthSnapshot(month: LibraryMonthKey)
    case acceptedSnapshotMissingBarrierCoverage
    case noDeleteCandidates
    case snapshotListFailed
    case candidateReadFailed(filename: String)
    case candidateCorruptOrUntrusted(filename: String)
    case candidateHeaderMismatch(filename: String, reason: RepoSnapshotCandidateHeaderMismatchReason)
    case unparseableSnapshotPresent(filename: String)
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
    var protectedByPolicyCount: Int = 0
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
    let barrierSet: RetentionBarrierSet
    let composedLivenessGate: RetentionLivenessGate
    let livenessDecision: RepoSnapshotDeletePreflightService.LivenessDecision
    let protectedFilenames: Set<String>
    let snapshotsToDelete: [RepoSnapshotDeleteCandidate]
    let protectedSummary: RepoSnapshotProtectedSummary
    let postDeleteContract: RepoSnapshotPostDeleteEquivalenceContract
}

struct RepoSnapshotDeletePreflightReport: Equatable, Sendable {
    let month: LibraryMonthKey
    let repoID: String
    let evaluatedAtMs: Int64
    var versionStatus: RepoRetentionPreflightVersionStatus?
    var remoteRepoID: String?
    var migrationMarkerPresent: Bool?
    var monthPartialMigrationMarkerPresent: Bool?
    var barrierLoad: RetentionManifestBarrierLoadResult?
    var composedLivenessGate: RetentionLivenessGate?
    var livenessDecision: RepoSnapshotDeletePreflightService.LivenessDecision?
    var acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo?
    var materializedCovered: CoveredRanges?
    var observedSeqByWriter: [String: UInt64] = [:]
    var protectedFilenames: Set<String> = []
    var candidateScan: RepoSnapshotDeleteCandidateScanResult?
}

enum RepoSnapshotDeletePreflightResult: Equatable, Sendable {
    case blocked(blockers: [RepoSnapshotDeletePreflightBlocker], report: RepoSnapshotDeletePreflightReport)
    case planned(plan: RepoSnapshotDeletePreflightPlan, report: RepoSnapshotDeletePreflightReport)
}

struct RepoSnapshotDeletePreflightService: Sendable {

    struct LivenessDecision: Equatable, Sendable {
        var blockers: [String] = []
        var evaluatedAtMs: Int64
        var allowed: Bool { blockers.isEmpty }
    }

    let client: any RemoteStorageClientProtocol
    let basePath: String
    let policy: RepoCompactionPolicy
    let isLocalVolume: Bool
    let barrierClockSkewToleranceMs: Int64

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        policy: RepoCompactionPolicy = .default,
        isLocalVolume: Bool,
        barrierClockSkewToleranceMs: Int64 = 5 * 60 * 1000
    ) {
        self.client = wrapIfSerial(client)
        self.basePath = basePath
        self.policy = policy
        self.isLocalVolume = isLocalVolume
        self.barrierClockSkewToleranceMs = barrierClockSkewToleranceMs
    }

    func makePlan(
        month: LibraryMonthKey,
        expectedRepoID: String,
        nowMs: Int64
    ) async throws -> RepoSnapshotDeletePreflightResult {
        let repoID = RepoCanonicalIdentity.normalizeLossy(expectedRepoID)
        var report = RepoSnapshotDeletePreflightReport(
            month: month,
            repoID: repoID,
            evaluatedAtMs: nowMs
        )
        var blockers: [RepoSnapshotDeletePreflightBlocker] = []
        blockers.append(contentsOf: try await checkVersion(report: &report))
        blockers.append(contentsOf: try await checkMigrationMarkers(report: &report))
        blockers.append(contentsOf: try await checkMonthPartialMigrationMarker(month: month, report: &report))
        if !blockers.isEmpty {
            return .blocked(blockers: blockers, report: report)
        }

        let barrierLoad: RetentionManifestBarrierLoadResult
        do {
            barrierLoad = try await RetentionManifestRemoteStore(client: client, basePath: basePath)
                .loadBarrierSet(expectedRepoID: repoID, month: month)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .blocked(blockers: [.barrierSetReadFailed], report: report)
        }
        report.barrierLoad = barrierLoad
        if !barrierLoad.isComplete {
            blockers.append(.invalidBarrierSet(barrierLoad.invalid))
        }
        let allBarrierSet = barrierLoad.barrierSet
        if allBarrierSet.unsuperseded.isEmpty {
            blockers.append(.emptyBarrierSet)
        }
        blockers.append(contentsOf: barrierFutureBlockers(barrierSet: allBarrierSet, nowMs: nowMs))
        let barrierSet = deletionEligibleBarrierSet(validManifests: barrierLoad.valid, nowMs: nowMs)
        if barrierSet.unsuperseded.isEmpty {
            blockers.append(contentsOf: barrierTooYoungBlockers(barrierSet: allBarrierSet, nowMs: nowMs))
        }
        if !blockers.isEmpty {
            return .blocked(blockers: blockers, report: report)
        }

        let composedLivenessGate = barrierSet.composedLivenessGate
        report.composedLivenessGate = composedLivenessGate
        // Compute the retained-barrier union now so pre-delete evidence validation covers the
        // same set the protection contract later uses (eligible + fresh too-young unsuperseded).
        // Authorization and liveness gates continue to use the age-eligible `barrierSet` only.
        let retainedManifestsUnion = retainedBarrierManifests(
            eligible: barrierSet,
            allValid: allBarrierSet
        )
        // Retained barrier checkpoint evidence must be present, readable, and consistent
        // BEFORE any irreversible deletion. Validate the same union F5 protects post-delete;
        // otherwise a fresh too-young barrier's missing/tampered checkpoint would only be
        // caught by the post-delete verifier — after irreversible mutation.
        blockers.append(contentsOf: try await barrierCheckpointEvidenceBlockers(manifests: retainedManifestsUnion))
        blockers.append(contentsOf: livenessBlockers(
            composedGate: composedLivenessGate,
            nowMs: nowMs,
            report: &report
        ))
        if !blockers.isEmpty {
            return .blocked(blockers: blockers, report: report)
        }

        // Authoritative identity check against finalized identity (not materializer echo).
        do {
            switch try await RepoCanonicalIdentityReader(client: client, basePath: basePath).loadCanonicalProvenV2() {
            case .absent:
                return .blocked(
                    blockers: [.repoIdentityMismatch(expected: repoID, observed: "(absent)")],
                    report: report
                )
            case .found(let remoteID):
                let canonical = RepoCanonicalIdentity.normalizeLossy(remoteID)
                report.remoteRepoID = canonical
                guard canonical == repoID else {
                    return .blocked(
                        blockers: [.repoIdentityMismatch(expected: repoID, observed: canonical)],
                        report: report
                    )
                }
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .blocked(blockers: [.materializerReadFailed], report: report)
        }

        let materialized: RepoMaterializer.MaterializeOutput
        do {
            materialized = try await RepoMaterializer(client: client, basePath: basePath)
                .materializeMonth(month, expectedRepoID: repoID)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            if error is RepoMaterializer.MetadataReadRaceError {
                return .blocked(blockers: [.materializerReadRace], report: report)
            }
            return .blocked(blockers: [.materializerReadFailed], report: report)
        }

        guard let acceptedSnapshot = materialized.acceptedSnapshotBaselinesByMonth[month] else {
            return .blocked(blockers: [.noAcceptedPerMonthSnapshot(month: month)], report: report)
        }
        let materializedCovered = materialized.coveredByMonth[month, default: .empty]
        report.acceptedSnapshot = acceptedSnapshot
        report.materializedCovered = materializedCovered
        report.observedSeqByWriter = materialized.observedSeqByWriter

        guard acceptedSnapshot.covered.superset(of: barrierSet.unionCovered) else {
            return .blocked(blockers: [.acceptedSnapshotMissingBarrierCoverage], report: report)
        }
        // Barrier-attested observed-seq must not regress in the live materializer view.
        // Parity with commit-prefix preflight so both share the same trust boundary.
        let barrierObservedSeq = barrierObservedSeqHighByWriter(barrierSet: barrierSet)
        let observedRegressionBlockers = observedSeqRegressionBlockers(
            required: barrierObservedSeq,
            observed: materialized.observedSeqByWriter
        )
        if !observedRegressionBlockers.isEmpty {
            return .blocked(blockers: observedRegressionBlockers, report: report)
        }
        let requiredObservedSeqByWriter = mergedObservedSeqRequirements(
            materialized: materialized.observedSeqByWriter,
            barrierObserved: barrierObservedSeq
        )

        // Read the accepted baseline once to capture SHA for the post-delete contract.
        let acceptedSnapshotFile: SnapshotFile
        do {
            acceptedSnapshotFile = try await SnapshotReader(client: client, basePath: basePath)
                .read(filename: acceptedSnapshot.filename)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            return .blocked(blockers: [.materializerReadFailed], report: report)
        }

        // Reuse the union computed above for pre-delete evidence validation. Union of (a)
        // age-eligible barriers' checkpoints — these authorize the current deletion, and (b)
        // all valid unsuperseded barriers' checkpoints — fresh too-young barriers that supersede
        // the eligible ones in the full set still have authoritative checkpoint evidence that
        // must stay protected until that barrier itself ages into eligibility.
        let barrierReferenced: Set<String> = Set(retainedManifestsUnion.map(\.checkpointSnapshotName))
        let scanner = SnapshotDeleteCandidateScanner(
            client: client,
            basePath: basePath,
            policy: policy
        )
        let scan: RepoSnapshotDeleteCandidateScanResult
        do {
            scan = try await scanner.scan(
                month: month,
                expectedRepoID: repoID,
                acceptedBaseline: acceptedSnapshot,
                barrierReferencedFilenames: barrierReferenced
            )
        } catch is CancellationError {
            throw CancellationError()
        }
        report.candidateScan = scan
        if !scan.blockers.isEmpty {
            return .blocked(blockers: scan.blockers, report: report)
        }
        if !scan.acceptedBaselineListed {
            return .blocked(
                blockers: [.acceptedBaselineNotListed(filename: acceptedSnapshot.filename)],
                report: report
            )
        }
        if scan.candidates.isEmpty {
            return .blocked(blockers: [.noDeleteCandidates], report: report)
        }

        // Retained barriers stamp the deletion authorization contract; their published
        // `policy.snapshotKeepCount` is the floor for fallback retention even if the current
        // runtime policy is lower (e.g. an app update changed the constant). Take the max so
        // narrowing the local policy after a barrier is already published does not retroactively
        // permit deleting snapshots the barrier authorized to keep. Floor from both eligible and
        // too-young unsuperseded barriers — a fresh barrier's keepCount still binds the runtime.
        let retainedSnapshotKeepCountMax = retainedManifestsUnion.reduce(0) { partial, manifest in
            max(partial, manifest.policy.snapshotKeepCount)
        }
        let effectiveSnapshotKeepCount = max(policy.snapshotFallbackKeepCount, retainedSnapshotKeepCountMax)
        let protectionInput = RepoSnapshotProtectionSet.Input(
            acceptedBaselineFilename: acceptedSnapshot.filename,
            acceptedBaselineCovered: acceptedSnapshot.covered,
            barrierReferencedFilenames: barrierReferenced,
            parseableSnapshotsForMonth: scan.parseableSnapshots.map {
                RepoSnapshotProtectionSet.Input.Parseable(
                    filename: $0.filename,
                    lamport: $0.lamport,
                    writerID: $0.writerID,
                    covered: $0.covered
                )
            },
            snapshotKeepCount: effectiveSnapshotKeepCount
        )
        let protection = RepoSnapshotProtectionSet.compute(protectionInput)
        report.protectedFilenames = protection.protectedFilenames

        // Final delete list = scanner candidates minus anything the protection rules add.
        let deleteCandidates = scan.candidates
            .filter { !protection.protectedFilenames.contains($0.filename) }
        if deleteCandidates.isEmpty {
            return .blocked(blockers: [.noDeleteCandidates], report: report)
        }

        // Include checkpoints from both eligible and too-young unsuperseded barriers in the
        // post-delete contract so the verifier confirms every retained barrier's evidence
        // survived the deletion, not just the ones that authorized it. `Dictionary(_:uniquingKeysWith:)`
        // keeps the last entry's SHA on collision — same checkpoint filename → same SHA in practice,
        // so the dedup is content-stable.
        let retainedSHAByFilename: [String: String] = Dictionary(
            retainedManifestsUnion.map {
                ($0.checkpointSnapshotName, $0.checkpointSHA256Hex)
            },
            uniquingKeysWith: { _, new in new }
        )

        // Any protected snapshot that is neither the accepted baseline nor a barrier
        // checkpoint (e.g. fallback-protected by keepN) still must survive deletion; the
        // verifier needs its SHA to detect tampering or disappearance. SHAs below the
        // baseline come from the scanner; above-baseline ones (excluding the baseline)
        // require an extra read.
        var additionalProtectedSHAByFilename: [String: String] = [:]
        let candidateSHAByFilename = Dictionary(uniqueKeysWithValues: scan.candidates.map {
            ($0.filename, $0.sha256Hex.lowercased())
        })
        let snapshotReader = SnapshotReader(client: client, basePath: basePath)
        for filename in protection.protectedFilenames.sorted() {
            if filename == acceptedSnapshot.filename { continue }
            if retainedSHAByFilename[filename] != nil { continue }
            if let sha = candidateSHAByFilename[filename] {
                additionalProtectedSHAByFilename[filename] = sha
                continue
            }
            do {
                let file = try await snapshotReader.read(filename: filename)
                additionalProtectedSHAByFilename[filename] = file.sha256Hex.lowercased()
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                return .blocked(blockers: [.materializerReadFailed], report: report)
            }
        }

        let contract = RepoSnapshotPostDeleteEquivalenceContract(
            acceptedSnapshotFilename: acceptedSnapshot.filename,
            acceptedSnapshotLamport: acceptedSnapshot.lamport,
            acceptedSnapshotSHA256Hex: acceptedSnapshotFile.sha256Hex.lowercased(),
            acceptedSnapshotCovered: acceptedSnapshot.covered,
            retainedBarrierUnionCovered: barrierSet.unionCovered,
            retainedManifestCheckpointSHA256ByFilename: retainedSHAByFilename,
            additionalProtectedSnapshotSHA256ByFilename: additionalProtectedSHAByFilename,
            requiredObservedSeqByWriter: requiredObservedSeqByWriter,
            preDeleteCovered: materializedCovered,
            preDeleteState: materialized.state,
            preDeleteObservedClock: materialized.state.observedClock
        )

        let plan = RepoSnapshotDeletePreflightPlan(
            month: month,
            repoID: repoID,
            acceptedSnapshot: acceptedSnapshot,
            acceptedSnapshotSHA256Hex: acceptedSnapshotFile.sha256Hex.lowercased(),
            barrierSet: barrierSet,
            composedLivenessGate: composedLivenessGate,
            livenessDecision: report.livenessDecision ?? RepoSnapshotDeletePreflightService.LivenessDecision(
                blockers: [],
                evaluatedAtMs: nowMs
            ),
            protectedFilenames: protection.protectedFilenames,
            snapshotsToDelete: deleteCandidates,
            protectedSummary: scan.protectedSummary,
            postDeleteContract: contract
        )
        return .planned(plan: plan, report: report)
    }

    /// Union of unsuperseded barriers from the age-eligible set (deletion-authorizing) and the
    /// full valid set (all currently-active barriers). Deduplicates by manifest filename so the
    /// same barrier present in both sets is not counted twice.
    private func retainedBarrierManifests(
        eligible: RetentionBarrierSet,
        allValid: RetentionBarrierSet
    ) -> [RetentionManifest] {
        var seenFilenames: Set<String> = []
        var result: [RetentionManifest] = []
        for manifest in eligible.unsuperseded + allValid.unsuperseded {
            let filename = RetentionManifestStore.filename(for: manifest.ref)
            if seenFilenames.insert(filename).inserted {
                result.append(manifest)
            }
        }
        return result
    }

    // MARK: - Shared with commit-prefix delete

    private func deletionEligibleBarrierSet(
        validManifests: [RetentionManifest],
        nowMs: Int64
    ) -> RetentionBarrierSet {
        let minAgeMs = Int64(policy.retentionStalenessThresholdSeconds) * 1000
        return RetentionBarrierSet.unsuperseded(manifests: validManifests.filter { manifest in
            nowMs - manifest.createdAtMs >= minAgeMs
        })
    }

    private func checkVersion(
        report: inout RepoSnapshotDeletePreflightReport
    ) async throws -> [RepoSnapshotDeletePreflightBlocker] {
        do {
            switch try await VersionManifestStore(client: client, basePath: basePath).loadToleratingDownloadVisibilityLag() {
            case .absent:
                report.versionStatus = .missing
                return [.missingVersion]
            case .found(let manifest):
                do {
                    try VersionManifestStore.classify(
                        remoteFormat: manifest.formatVersion,
                        minAppVersion: manifest.minAppVersion
                    )
                    report.versionStatus = .compatible(formatVersion: manifest.formatVersion)
                    return []
                } catch RepoBootstrap.VersionConflict.higherFormatVersion(let remote, _, _) {
                    report.versionStatus = .unsupported(formatVersion: remote)
                    return [.unsupportedVersion(formatVersion: remote)]
                } catch RepoBootstrap.VersionConflict.mismatchedFormatVersion(let remote, _, _) {
                    report.versionStatus = .unsupported(formatVersion: remote)
                    return [.unsupportedVersion(formatVersion: remote)]
                } catch {
                    report.versionStatus = .unreadable
                    return [.unreadableVersion]
                }
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            report.versionStatus = .unreadable
            return [.unreadableVersion]
        }
    }

    private func checkMigrationMarkers(
        report: inout RepoSnapshotDeletePreflightReport
    ) async throws -> [RepoSnapshotDeletePreflightBlocker] {
        do {
            let exists: Bool
            let found = try await GracefulRead.retryWithinGrace(
                client: client,
                floorSeconds: 1,
                backoff: .exponential(baseMs: 200, maxShift: 3)
            ) {
                let found = try await MigrationMarkerStore(client: client, basePath: basePath).existsAny()
                return found ? true : nil
            }
            exists = found != nil
            report.migrationMarkerPresent = exists
            return exists ? [.migrationInProgress] : []
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            report.migrationMarkerPresent = nil
            return [.migrationCheckFailed]
        }
    }

    private func checkMonthPartialMigrationMarker(
        month: LibraryMonthKey,
        report: inout RepoSnapshotDeletePreflightReport
    ) async throws -> [RepoSnapshotDeletePreflightBlocker] {
        let markerPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: String(format: "%04d/%02d/%@", month.year, month.month, V1MigrationResidueFileNames.partialMigrationMarkerFileName)
        )
        let entry: RemoteStorageEntry?
        do {
            entry = try await GracefulRead.retryWithinGrace(
                client: client,
                floorSeconds: 1,
                backoff: .exponential(baseMs: 200, maxShift: 3)
            ) {
                do {
                    return try await client.metadata(path: markerPath)
                } catch {
                    if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                    guard isStorageNotFoundError(error) else { throw error }
                    return nil
                }
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            report.monthPartialMigrationMarkerPresent = nil
            return [.migrationResidueCheckFailed(month: month)]
        }
        guard let entry else {
            report.monthPartialMigrationMarkerPresent = false
            return []
        }
        if entry.isDirectory {
            report.monthPartialMigrationMarkerPresent = nil
            return [.migrationResidueCheckFailed(month: month)]
        }
        report.monthPartialMigrationMarkerPresent = true
        return [.migrationResiduePresent(month: month)]
    }

    private func barrierFutureBlockers(
        barrierSet: RetentionBarrierSet,
        nowMs: Int64
    ) -> [RepoSnapshotDeletePreflightBlocker] {
        var blockers: [RepoSnapshotDeletePreflightBlocker] = []
        for manifest in barrierSet.unsuperseded {
            let filename = RetentionManifestStore.filename(for: manifest.ref)
            if manifest.createdAtMs > nowMs + barrierClockSkewToleranceMs {
                blockers.append(.barrierCreatedInFuture(filename: filename, createdAtMs: manifest.createdAtMs))
            }
        }
        return blockers
    }

    private func barrierTooYoungBlockers(
        barrierSet: RetentionBarrierSet,
        nowMs: Int64
    ) -> [RepoSnapshotDeletePreflightBlocker] {
        let minAgeMs = Int64(policy.retentionStalenessThresholdSeconds) * 1000
        var blockers: [RepoSnapshotDeletePreflightBlocker] = []
        for manifest in barrierSet.unsuperseded {
            let filename = RetentionManifestStore.filename(for: manifest.ref)
            if manifest.createdAtMs <= nowMs + barrierClockSkewToleranceMs,
               nowMs - manifest.createdAtMs < minAgeMs {
                blockers.append(.barrierTooYoung(filename: filename, createdAtMs: manifest.createdAtMs))
            }
        }
        return blockers
    }

    private func barrierCheckpointEvidenceBlockers(
        manifests: [RetentionManifest]
    ) async throws -> [RepoSnapshotDeletePreflightBlocker] {
        let reader = SnapshotReader(client: client, basePath: basePath)
        var blockers: [RepoSnapshotDeletePreflightBlocker] = []
        for manifest in manifests.sorted(by: { lhs, rhs in
            RetentionManifestStore.filename(for: lhs.ref) < RetentionManifestStore.filename(for: rhs.ref)
        }) {
            let filename = manifest.checkpointSnapshotName
            let snapshot: SnapshotFile
            do {
                snapshot = try await reader.read(filename: filename)
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                blockers.append(.barrierCheckpointReadFailed(filename: filename))
                continue
            }
            if let mismatch = barrierCheckpointMismatch(manifest: manifest, snapshot: snapshot) {
                blockers.append(.barrierCheckpointMismatch(filename: filename, reason: mismatch))
            }
        }
        return blockers
    }

    private func barrierCheckpointMismatch(
        manifest: RetentionManifest,
        snapshot: SnapshotFile
    ) -> RepoRetentionBarrierCheckpointMismatchReason? {
        if snapshot.sha256Hex.lowercased() != manifest.checkpointSHA256Hex {
            return .sha256(expected: manifest.checkpointSHA256Hex, actual: snapshot.sha256Hex.lowercased())
        }
        if RepoCanonicalIdentity.normalizeLossy(snapshot.header.repoID) != manifest.repoID {
            return .repoID(
                expected: manifest.repoID,
                actual: RepoCanonicalIdentity.normalizeLossy(snapshot.header.repoID)
            )
        }
        let snapshotMonth = CommitHeader.parseMonthScope(snapshot.header.scope)
        if snapshotMonth != manifest.month {
            return .month(expected: manifest.month, actual: snapshotMonth)
        }
        if snapshot.header.writerID != manifest.createdByWriterID {
            return .writerID(expected: manifest.createdByWriterID, actual: snapshot.header.writerID)
        }
        if snapshot.header.covered != manifest.coveredRanges {
            return .coveredRanges
        }
        return nil
    }

    private func barrierObservedSeqHighByWriter(barrierSet: RetentionBarrierSet) -> [String: UInt64] {
        var result: [String: UInt64] = [:]
        for manifest in barrierSet.unsuperseded {
            for (writerID, seq) in manifest.observedSeqHighByWriter {
                result[writerID] = max(result[writerID] ?? 0, seq)
            }
        }
        return result
    }

    private func observedSeqRegressionBlockers(
        required: [String: UInt64],
        observed: [String: UInt64]
    ) -> [RepoSnapshotDeletePreflightBlocker] {
        required.keys.sorted().compactMap { writerID in
            let expected = required[writerID] ?? 0
            let actual = observed[writerID] ?? 0
            guard actual < expected else { return nil }
            return .barrierObservedSeqRegression(
                writerID: writerID,
                expectedAtLeast: expected,
                observed: actual
            )
        }
    }

    private func mergedObservedSeqRequirements(
        materialized: [String: UInt64],
        barrierObserved: [String: UInt64]
    ) -> [String: UInt64] {
        var result = materialized
        for (writerID, seq) in barrierObserved {
            result[writerID] = max(result[writerID] ?? 0, seq)
        }
        return result
    }

    private func livenessBlockers(
        composedGate: RetentionLivenessGate,
        nowMs: Int64,
        report: inout RepoSnapshotDeletePreflightReport
    ) -> [RepoSnapshotDeletePreflightBlocker] {
        return []
    }
}

/// Body-level scanner for snapshot delete candidates. Blocks the whole month on any
/// unreadable / corrupt / foreign-repo / header-mismatch parseable target-month snapshot.
/// Mirrors `RepoRetentionDeleteCandidateScanner` (commits) for safety parity.
struct SnapshotDeleteCandidateScanner: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String
    let policy: RepoCompactionPolicy

    func scan(
        month: LibraryMonthKey,
        expectedRepoID: String,
        acceptedBaseline: RepoMaterializer.AcceptedSnapshotBaselineInfo,
        barrierReferencedFilenames: Set<String>
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
                // A directory squatting at a target-month snapshot filename is damaged remote
                // state, not "snapshot absent" — fail closed so snapshot GC can't run while
                // target-month snapshot metadata is uncertain. Parallel to the file-shaped
                // unparseable handling below.
                if entry.name.hasSuffix(".jsonl"),
                   let monthHint = monthPrefix(from: entry.name), monthHint == month {
                    protectedSummary.unparseableSnapshotsForMonth += 1
                    blockers.append(.unparseableSnapshotPresent(filename: entry.name))
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
                // Unparseable filename: if it could be a target-month entry, block month.
                if let monthHint = monthPrefix(from: entry.name), monthHint == month {
                    protectedSummary.unparseableSnapshotsForMonth += 1
                    protectedSummary.protectedBytes += entry.size
                    blockers.append(.unparseableSnapshotPresent(filename: entry.name))
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

            // Body-level trust.
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
                header: snapshotFile.header,
                expectedRepoID: expectedRepoID
            ) {
                protectedSummary.headerMismatchCandidateCount += 1
                protectedSummary.protectedBytes += entry.size
                blockers.append(.candidateHeaderMismatch(filename: entry.name, reason: mismatch))
                continue
            }

            // Apply materializer's semantic snapshot-baseline trust predicates so the
            // scanner never proposes a candidate the materializer would reject as a
            // trusted baseline. Failing any of these blocks the whole month, since the
            // body is corrupt-by-semantics even if JSONL integrity passed.
            if !snapshotBodyIsMaterializerTrusted(snapshotFile, month: parsed.month, filenameLamport: parsed.lamport) {
                protectedSummary.corruptOrUntrustedCandidateCount += 1
                protectedSummary.protectedBytes += entry.size
                blockers.append(.candidateCorruptOrUntrusted(filename: entry.name))
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

            // Covered-dominance candidate selection: delete only when the accepted baseline
            // fully covers the candidate. Incomparable candidates (where neither is a superset)
            // or candidates covering ranges the accepted baseline does not must remain protected.
            let acceptedCoversCandidate = acceptedBaseline.covered.superset(of: snapshotFile.header.covered)
            if acceptedCoversCandidate {
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
            } else {
                if barrierReferencedFilenames.contains(entry.name) {
                    protectedSummary.protectedByPolicyCount += 1
                }
            }
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

/// Mirrors the trust predicates the materializer applies before accepting a
/// per-month snapshot as a baseline. A snapshot that any of these reject would
/// be silently dropped by the materializer, so it must not be deletable either.
/// Predicates duplicated (not re-exported) to keep `RepoMaterializer`'s surface
/// unchanged; behavior must stay aligned with the originals if they evolve.
private func snapshotBodyIsMaterializerTrusted(
    _ file: SnapshotFile,
    month: LibraryMonthKey,
    filenameLamport: UInt64
) -> Bool {
    // Resource rows: physicalRemotePath must belong to the same month.
    for resource in file.resources {
        if !snapshotResourcePathBelongsTo(resource.physicalRemotePath, month: month) {
            return false
        }
    }
    // Row stamps: clock not poisoned, seq within header.covered for the writer.
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
        // Materializer only accepts asset tombstones; non-.asset or malformed hashes
        // would cause makeBaseline to return nil.
        guard deletedKey.keyType == .asset else { return false }
        do {
            _ = try RepoWireValidator.validateHash(deletedKey.keyValue, field: "keyValue")
        } catch {
            return false
        }
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
