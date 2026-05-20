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
    let fresh: Bool
    let missingByMonth: [LibraryMonthKey: Set<Data>]
    let freshMonths: Set<LibraryMonthKey>
}

struct RemoteIndexPhysicalPresenceOverlayProbe: Sendable {
    func probe(
        snapshot: RemoteLibrarySnapshot,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        fallback: [LibraryMonthKey: Set<Data>],
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
        var missingByMonth: [LibraryMonthKey: Set<Data>] = [:]
        var freshMonths: Set<LibraryMonthKey> = []
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
                    if !probe.inconclusiveHashes.isEmpty {
                        if let stale = fallback[month] {
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
                    if monthFresh {
                        freshMonths.insert(month)
                    } else {
                        anyFailure = true
                    }
                    if monthFresh || !missing.isEmpty {
                        missingByMonth[month] = missing
                    }
                case .failure(let error):
                    anyFailure = true
                    if let stale = fallback[month] {
                        missingByMonth[month] = stale
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
            fresh: !anyFailure,
            missingByMonth: missingByMonth,
            freshMonths: freshMonths
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
                for resource in resources {
                    presence[resource.contentHash] = .inconclusive(.probeFailure)
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
        for entry in entries where !entry.isDirectory {
            entriesByKey[nameCase.presenceKey(for: entry.name), default: []].append(ListedFile(name: entry.name, size: entry.size))
        }
        var resourcesByHash: [Data: [RemoteManifestResource]] = [:]
        for resource in resources {
            resourcesByHash[resource.contentHash, default: []].append(resource)
        }
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
                guard let listed = entriesByKey[nameCase.presenceKey(for: leaf)] else { continue }
                let sizeMatches = listed.filter { $0.size == resource.fileSize }
                if sizeMatches.isEmpty { continue }
                for match in sizeMatches {
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
                    let path = RemotePathBuilder.absolutePath(
                        basePath: basePath,
                        remoteRelativePath: monthRel + "/" + match.name
                    )
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
