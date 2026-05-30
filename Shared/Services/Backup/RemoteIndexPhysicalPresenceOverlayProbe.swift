import Foundation
import os.log

private let overlayProbeLog = Logger(subsystem: "com.zizicici.watermelon", category: "SyncTiming")

struct RemoteIndexOverlayProbeBudget: Sendable {
    let maxVerifiedFilesPerMonth: Int
    let maxVerifiedBytesPerMonth: Int64
}

enum RemoteIndexOverlayStaleFallbackPolicy: Sendable {
    case failClosedWhenMissingFallback
    case preserveFallback
}

struct RemoteIndexOverlayMonthProbe: Sendable {
    let month: LibraryMonthKey
    let presenceByHash: [Data: RemoteResourcePresence]

    var missingHashes: Set<Data> {
        var result: Set<Data> = []
        for (hash, presence) in presenceByHash where presence == .missing {
            result.insert(hash)
        }
        return result
    }

    var inconclusiveHashes: Set<Data> {
        var result: Set<Data> = []
        for (hash, presence) in presenceByHash {
            if case .inconclusive = presence { result.insert(hash) }
        }
        return result
    }

    func inconclusiveHashes(reason needle: RemoteResourcePresence.InconclusiveReason) -> Set<Data> {
        var result: Set<Data> = []
        for (hash, presence) in presenceByHash {
            if case .inconclusive(let r) = presence, r == needle {
                result.insert(hash)
            }
        }
        return result
    }
}

struct RemoteIndexOverlayProbeResult: Sendable {
    let allMonthsFresh: Bool
    let presence: RemotePresenceSnapshot
}

struct RemoteIndexPhysicalPresenceOverlayProbe: Sendable {
    func probe(
        snapshot: RemoteLibrarySnapshot,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        fallback: RemotePresenceSnapshot,
        budget: RemoteIndexOverlayProbeBudget?,
        staleFallbackPolicy: RemoteIndexOverlayStaleFallbackPolicy,
        concurrencyCap: Int
    ) async throws -> RemoteIndexOverlayProbeResult {
        var resourcesByMonth: [LibraryMonthKey: [RemoteManifestResource]] = [:]
        for resource in snapshot.resources {
            let month = LibraryMonthKey(year: resource.year, month: resource.month)
            resourcesByMonth[month, default: []].append(resource)
        }
        let effectiveCap = client.concurrencyMode == .serialOnly ? 1 : concurrencyCap
        var iterator = resourcesByMonth.makeIterator()
        var anyFailure = false
        var builder = RemotePresenceSnapshot.Builder()
        try await withThrowingTaskGroup(of: (LibraryMonthKey, Result<RemoteIndexOverlayMonthProbe, Error>).self) { group in
            for _ in 0..<effectiveCap {
                guard let (month, resources) = iterator.next() else { break }
                group.addTask {
                    try Task.checkCancellation()
                    do {
                        let probe = try await probeMonthForMissing(client: client, basePath: basePath, month: month, resources: resources, budget: budget)
                        return (probe.month, .success(probe))
                    } catch {
                        if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                        return (month, .failure(error))
                    }
                }
            }
            while let (month, result) = try await group.next() {
                try Task.checkCancellation()
                switch result {
                case .success(let probe):
                    var missing = probe.missingHashes
                    var resolvedInconclusives: Set<Data> = []
                    let stale = fallback.month(month).missingHashes
                    if !probe.inconclusiveHashes.isEmpty {
                        if !stale.isEmpty {
                            let cover = stale.intersection(probe.inconclusiveHashes)
                            missing.formUnion(cover)
                            let budgetExhaustedCover = cover.intersection(
                                probe.inconclusiveHashes(reason: .verifyBudgetExhausted)
                            )
                            resolvedInconclusives.formUnion(budgetExhaustedCover)
                        }
                        if case .failClosedWhenMissingFallback = staleFallbackPolicy {
                            let budgetExhausted = probe.inconclusiveHashes(reason: .verifyBudgetExhausted)
                            let unresolved = budgetExhausted.subtracting(resolvedInconclusives)
                            missing.formUnion(unresolved)
                            resolvedInconclusives.formUnion(unresolved)
                        }
                    }
                    let monthFresh: Bool
                    switch staleFallbackPolicy {
                    case .failClosedWhenMissingFallback:
                        monthFresh = probe.inconclusiveHashes.subtracting(resolvedInconclusives).isEmpty
                    case .preserveFallback:
                        monthFresh = probe.inconclusiveHashes.isEmpty
                    }
                    if !monthFresh {
                        anyFailure = true
                    }
                    // Without this, a stale prior-missing hash that the probe just verified present
                    // would survive in the overlay whenever any unrelated hash stayed inconclusive
                    // (e.g. budget exhausted on a different hash) — Home would keep treating a
                    // healthy remote asset as incomplete and queue a repair upload.
                    if monthFresh || !missing.isEmpty || !stale.isEmpty {
                        builder.set(month, missingHashes: missing, isAuthoritative: monthFresh)
                    }
                case .failure(let error):
                    anyFailure = true
                    let stale = fallback.month(month).missingHashes
                    if !stale.isEmpty {
                        builder.set(month, missingHashes: stale, isAuthoritative: false)
                    }
                    overlayProbeLog.info("[SyncTiming] probe failed for \(month.text): \(error.localizedDescription)")
                }
                if let (nextMonth, nextResources) = iterator.next() {
                    group.addTask {
                        try Task.checkCancellation()
                        do {
                            let probe = try await probeMonthForMissing(client: client, basePath: basePath, month: nextMonth, resources: nextResources, budget: budget)
                            return (probe.month, .success(probe))
                        } catch {
                            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                            return (nextMonth, .failure(error))
                        }
                    }
                }
            }
        }
        return RemoteIndexOverlayProbeResult(
            allMonthsFresh: !anyFailure,
            presence: builder.build()
        )
    }

    private func probeMonthForMissing(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        budget: RemoteIndexOverlayProbeBudget?
    ) async throws -> RemoteIndexOverlayMonthProbe {
        try Task.checkCancellation()
        let monthRel = String(format: "%04d/%02d", month.year, month.month)
        let monthAbs = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRel)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbs)
        } catch {
            if isStorageNotFoundError(error) {
                var presence: [Data: RemoteResourcePresence] = [:]
                let graceSeconds = client.readAfterWriteGraceSeconds
                if graceSeconds > 0 {
                    // Grace backend: gate each resource on freshness (mirrors the file-level recorded-path
                    // probe and verify's month-dir gate). An old resource whose whole month dir is gone is
                    // genuinely missing so Home can repair it; a within-grace resource could be a
                    // just-written month not yet listed → inconclusive (keeps the month non-authoritative).
                    let now = Date()
                    var withinGraceByHash: [Data: Bool] = [:]
                    for resource in resources {
                        let within = RepoVerifyMonthService.isWithinGraceWindow(
                            backedUpAtMs: resource.backedUpAtMs, now: now, graceSeconds: graceSeconds
                        )
                        withinGraceByHash[resource.contentHash] = (withinGraceByHash[resource.contentHash] ?? false) || within
                    }
                    for (hash, within) in withinGraceByHash {
                        presence[hash] = within ? .inconclusive(.probeFailure) : .missing
                    }
                } else {
                    // Zero-grace: a whole-month 404 stays a transient probe failure rather than widening
                    // every recorded hash to missing off one listing.
                    for resource in resources {
                        presence[resource.contentHash] = .inconclusive(.probeFailure)
                    }
                }
                return RemoteIndexOverlayMonthProbe(month: month, presenceByHash: presence)
            }
            throw error
        }
        try Task.checkCancellation()
        let nameCase = client.backendNameCaseSensitivity
        struct ListedFile {
            let name: String
            let size: Int64
        }
        var entriesByKey: [String: [ListedFile]] = [:]
        var entriesByCanonicalKey: [String: [ListedFile]] = [:]
        for entry in entries where !entry.isDirectory {
            entriesByKey[nameCase.presenceKey(for: entry.name), default: []].append(ListedFile(name: entry.name, size: entry.size))
            entriesByCanonicalKey[nameCase.canonicalEquivalenceKey(for: entry.name), default: []].append(ListedFile(name: entry.name, size: entry.size))
        }
        var resourcesByHash: [Data: [RemoteManifestResource]] = [:]
        for resource in resources {
            resourcesByHash[resource.contentHash, default: []].append(resource)
        }
        let graceSeconds = client.readAfterWriteGraceSeconds
        let graceBackend = graceSeconds > 0
        let now = Date()
        var verifiedFileCount = 0
        var verifiedByteCount: Int64 = 0
        var loggedProbeBudgetExhausted = false
        var presenceByHash: [Data: RemoteResourcePresence] = [:]
        for (hash, group) in resourcesByHash {
            var anyPresent = false
            var inconclusiveReason: RemoteResourcePresence.InconclusiveReason?
            candidateScan: for resource in group {
                try Task.checkCancellation()
                let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
                let listed = entriesByKey[nameCase.presenceKey(for: leaf)] ?? []
                let sizeMatches = listed.filter { $0.size == resource.fileSize }
                let probePaths: [String]
                // A grace recorded-path probe found nothing but the resource is older than the
                // read-after-write window: the 404 is genuine absence, so let it fall through to
                // .missing instead of latching inconclusive (which keeps the month non-authoritative forever).
                var staleRecordedPathProbe = false
                if sizeMatches.isEmpty {
                    // Byte-exact key missed. A listed canonically-equivalent same-size leaf (a normalizing
                    // HFS+/SFTP server that stored our NFC leaf as NFD), or — on a grace backend — a stale
                    // LIST that hid a just-written file, is only a reason to probe. Probe the *recorded*
                    // path that restore uses, never a listed sibling: an exact-name backend can list an
                    // unrelated same-hash orphan under an equivalent spelling while the committed object is
                    // gone, and proving presence from that orphan marks a non-restorable asset healthy.
                    let hasCanonicalMatch = !(entriesByCanonicalKey[nameCase.canonicalEquivalenceKey(for: leaf)] ?? [])
                        .filter { $0.size == resource.fileSize }.isEmpty
                    guard hasCanonicalMatch || graceBackend else { continue }
                    staleRecordedPathProbe = !RepoVerifyMonthService.isWithinGraceWindow(
                        backedUpAtMs: resource.backedUpAtMs, now: now, graceSeconds: graceSeconds
                    )
                    probePaths = [RemotePathBuilder.absolutePath(
                        basePath: basePath,
                        remoteRelativePath: resource.physicalRemotePath
                    )]
                } else {
                    probePaths = sizeMatches.map { match in
                        RemotePathBuilder.absolutePath(
                            basePath: basePath,
                            remoteRelativePath: monthRel + "/" + match.name
                        )
                    }
                }
                for path in probePaths {
                    if let budget {
                        let fileCap = verifiedFileCount >= budget.maxVerifiedFilesPerMonth
                        let byteCap = verifiedByteCount + max(resource.fileSize, 0) > budget.maxVerifiedBytesPerMonth
                        if fileCap || byteCap {
                            if !loggedProbeBudgetExhausted {
                                loggedProbeBudgetExhausted = true
                                overlayProbeLog.info("[SyncTiming] overlay probe budget exhausted for \(month.text); leaving unverified resources inconclusive")
                            }
                            inconclusiveReason = .verifyBudgetExhausted
                            break candidateScan
                        }
                    }
                    do {
                        switch try await RemoteContentTrust.verifyHashResult(
                            client: client,
                            remotePath: path,
                            expectedSize: resource.fileSize,
                            expectedHash: hash
                        ) {
                        case .matched:
                            verifiedFileCount += 1
                            verifiedByteCount += resource.fileSize
                            anyPresent = true
                            break candidateScan
                        case .mismatched:
                            verifiedFileCount += 1
                            verifiedByteCount += resource.fileSize
                        case .noContent:
                            continue
                        case .inconclusive:
                            // A not-found recorded-path probe past the grace window is genuine absence;
                            // fall through to .missing so Home repairs it instead of staying non-authoritative.
                            if staleRecordedPathProbe { continue }
                            inconclusiveReason = .probeFailure
                        }
                    } catch {
                        if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                        if isStorageNotFoundError(error) { continue }
                        throw error
                    }
                }
            }
            if anyPresent {
                presenceByHash[hash] = .hashVerified
            } else if let reason = inconclusiveReason {
                presenceByHash[hash] = .inconclusive(reason)
            } else {
                presenceByHash[hash] = .missing
            }
        }
        return RemoteIndexOverlayMonthProbe(month: month, presenceByHash: presenceByHash)
    }
}
