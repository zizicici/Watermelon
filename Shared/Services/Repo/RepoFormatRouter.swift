import Foundation

// Repo V2 (Lite) format router. Decides what an existing remote looks like before any write: a committed
// Lite repo, a legacy V1 tree to migrate, a damaged/foreign tree, or empty space safe to initialize.
// It fails closed: a probe that can't be resolved throws rather than guessing `.fresh`.
nonisolated enum RepoFormatDecision: Equatable, Sendable {
    case current          // committed version.json: format 2
    case fresh            // nothing here (or a half-created marker dir); safe to initialize
    case v1Migrate        // legacy V1 month manifests present, no committed version
    case damaged          // Lite month data with no committed version
    case malformedVersion // canonical version missing, but current version scratch can be recovered
    case unsupported(minAppVersion: String? = nil) // future/foreign committed format
}

enum RepoFormatRouterError: Error, Equatable {
    case probeFault(RemoteFaultLite.Category, detail: String? = nil)
}

struct RepoFormatProbe: Sendable {
    let decision: RepoFormatDecision
    let repoDirectoryEntries: [RemoteStorageEntry]?
    let monthsDirectoryEntries: [RemoteStorageEntry]?
}

struct RepoFormatRouter: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    // OS-indexer and file-browser artifacts that are not evidence of a foreign writer.
    private static let noiseFileNames: Set<String> = [
        ".DS_Store", "Thumbs.db", "desktop.ini", ".metadata_never_index"
    ]

    private static func isNoiseFileName(_ name: String) -> Bool {
        noiseFileNames.contains(name) || (name.hasPrefix("._") && name.count > 2)
    }

    func classify() async throws -> RepoFormatDecision {
        let probe = try await classifyProbe(collectCurrentMonthsListing: false)
        return probe.decision
    }

    func classifyDetailed() async throws -> RepoFormatProbe {
        try await classifyProbe(collectCurrentMonthsListing: true)
    }

    func classifyForRead() async throws -> RepoFormatDecision {
        switch try await readVersion() {
        case .current:
            return .current
        case .unsupported(let minAppVersion):
            return .unsupported(minAppVersion: minAppVersion)
        case .missing, .damaged:
            return try await classify()
        }
    }

    private func classifyProbe(collectCurrentMonthsListing: Bool) async throws -> RepoFormatProbe {
        let normalizedBase = RemotePathBuilder.normalizePath(basePath)
        let baseEntries: [RemoteStorageEntry]
        do {
            baseEntries = try await client.list(path: normalizedBase)
        } catch {
            let category = RemoteFaultLite.classify(error)
            if category == .notFound {
                return RepoFormatProbe(decision: .fresh, repoDirectoryEntries: nil, monthsDirectoryEntries: nil)
            }
            throw Self.probeFault(from: error, category: category)
        }

        let repoDirPresent = baseEntries.contains {
            $0.isDirectory && $0.name == RepoLayoutLite.repoDirectoryName
        }
        // A non-directory object occupying the reserved `.watermelon` marker path is foreign control state,
        // not empty space. On S3-compatible flat-key stores the base listing can surface both this object and
        // a same-stem `.watermelon/` prefix (e.g. because `.watermelon/locks/...` exists), so the directory
        // marker alone does not clear it. Only a committed current version is trusted over such an object; any
        // uncommitted state must fail closed rather than initialize a Lite repo (commit version.json) under the
        // still-occupied reserved path.
        let nonDirectoryMarkerPresent = baseEntries.contains {
            !$0.isDirectory && $0.name == RepoLayoutLite.repoDirectoryName
        }

        if repoDirPresent {
            let versionRead = try await readVersion()
            switch versionRead {
            case .current:
                guard collectCurrentMonthsListing else {
                    return RepoFormatProbe(decision: .current, repoDirectoryEntries: nil, monthsDirectoryEntries: nil)
                }
                let repoState = try await inspectRepoDirectory(
                    scanMonths: true,
                    ignoreMonthsListFault: true
                )
                // Committed version is the only format commit point: trust it and never scan V1.
                return repoState.probe(decision: .current)
            case .unsupported(let minAppVersion):
                return RepoFormatProbe(
                    decision: .unsupported(minAppVersion: minAppVersion),
                    repoDirectoryEntries: nil,
                    monthsDirectoryEntries: nil
                )
            case .damaged:
                let listedRepoState = try await inspectRepoDirectory(scanMonths: false)
                // Undecodable version.json is damaged; classifyUncommittedRepo is consulted only for its
                // directory entries (its decision can no longer be .unsupported and is treated as damaged).
                let uncommitted = try await classifyUncommittedRepo(
                    baseEntries: baseEntries,
                    preferLiteDamageOverV1: true,
                    repoDirectoryEntries: listedRepoState.repoDirectoryEntries
                )
                return RepoFormatProbe(
                    decision: .damaged,
                    repoDirectoryEntries: uncommitted.repoDirectoryEntries,
                    monthsDirectoryEntries: uncommitted.monthsDirectoryEntries
                )
            case .missing:
                let listedRepoState = try await inspectRepoDirectory(scanMonths: false)
                if listedRepoState.hasVersionFile {
                    return listedRepoState.probe(decision: .damaged)
                }

                // Uncommitted under an observed marker directory: a coexisting reserved-marker object must fail
                // closed before classifyUncommittedRepo can route .fresh/.v1Migrate and let the gateway commit
                // version.json over it.
                if nonDirectoryMarkerPresent {
                    return RepoFormatProbe(decision: .damaged, repoDirectoryEntries: nil, monthsDirectoryEntries: nil)
                }
                return try await classifyUncommittedRepo(
                    baseEntries: baseEntries,
                    preferLiteDamageOverV1: false
                )
            }
        }

        // No directory marker: a lone reserved-marker object is still foreign control state, never fresh space.
        if nonDirectoryMarkerPresent {
            return RepoFormatProbe(decision: .damaged, repoDirectoryEntries: nil, monthsDirectoryEntries: nil)
        }

        switch try await v1Evidence(baseEntries: baseEntries) {
        case .validManifest:
            return RepoFormatProbe(decision: .v1Migrate, repoDirectoryEntries: nil, monthsDirectoryEntries: nil)
        case .directoryCandidateOnly:
            // A directory occupying a canonical V1 manifest slot is damaged/foreign control state, not empty
            // space: fail closed so a write path cannot commit a Lite version marker over unresolved V1 state.
            return RepoFormatProbe(decision: .damaged, repoDirectoryEntries: nil, monthsDirectoryEntries: nil)
        case .none:
            return RepoFormatProbe(decision: .fresh, repoDirectoryEntries: nil, monthsDirectoryEntries: nil)
        }
    }

    private func classifyUncommittedRepo(
        baseEntries: [RemoteStorageEntry],
        preferLiteDamageOverV1: Bool,
        repoDirectoryEntries: [RemoteStorageEntry]? = nil
    ) async throws -> RepoFormatProbe {
        let repoState = try await inspectRepoDirectory(entries: repoDirectoryEntries)
        // An unknown child under reserved `.watermelon` is foreign/unresolved control state: fail closed before
        // any version.json commit route (malformedVersion recovery, V1 migrate, fresh init) can publish over it.
        if repoState.hasUnknownChild {
            return repoState.probe(decision: .damaged)
        }
        if repoState.hasMonthSqlite, try await hasRecoverableVersionScratch(entries: repoState.repoDirectoryEntries) {
            // A recoverable version scratch recovers an interrupted version commit, but must not bury unresolved
            // V1 control state: route by V1 evidence exactly as the no-scratch switch below. Valid V1 ⇒ interrupted
            // migration (.v1Migrate re-validates source drift); a directory at a V1 manifest slot ⇒ fail closed
            // (.damaged); no V1 ⇒ a real interrupted version commit (.malformedVersion).
            if !preferLiteDamageOverV1 {
                switch try await v1Evidence(baseEntries: baseEntries) {
                case .validManifest:
                    return repoState.probe(decision: .v1Migrate)
                case .directoryCandidateOnly:
                    return repoState.probe(decision: .damaged)
                case .none:
                    break
                }
            }
            return repoState.probe(decision: .malformedVersion)
        }
        if preferLiteDamageOverV1, repoState.hasMonthSqlite {
            return repoState.probe(decision: .damaged)
        }
        switch try await v1Evidence(baseEntries: baseEntries) {
        case .validManifest:
            return repoState.probe(decision: .v1Migrate)
        case .directoryCandidateOnly:
            // Damaged/foreign V1 control state (a directory at the V1 manifest slot) must not fall through to
            // .fresh and let the version marker commit over it.
            return repoState.probe(decision: .damaged)
        case .none:
            break
        }
        if repoState.hasMonthSqlite {
            return repoState.probe(decision: .damaged)
        }
        return repoState.probe(decision: .fresh)
    }

    // MARK: - Version

    private enum VersionRead {
        case current
        case missing
        case unsupported(minAppVersion: String?)
        case damaged
    }

    private func readVersion() async throws -> VersionRead {
        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.versionFileName)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await client.download(remotePath: versionPath, localURL: localURL)
        } catch {
            let category = RemoteFaultLite.classify(error)
            if category == .notFound { return .missing }
            throw Self.probeFault(from: error, category: category)
        }

        guard let data = try? Data(contentsOf: localURL) else {
            return .damaged
        }
        switch VersionManifestLite.compatibility(for: data) {
        case .readableWritable:
            return .current
        case .unsupported(let minAppVersion):
            return .unsupported(minAppVersion: minAppVersion)
        case .damaged:
            return .damaged
        }
    }

    private func hasRecoverableVersionScratch(entries listedEntries: [RemoteStorageEntry]? = nil) async throws -> Bool {
        let entries: [RemoteStorageEntry]
        if let listedEntries {
            entries = listedEntries
        } else {
            do {
                entries = try await client.list(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
            } catch {
                let category = RemoteFaultLite.classify(error)
                if category == .notFound { return false }
                throw Self.probeFault(from: error, category: category)
            }
        }

        for entry in entries where !entry.isDirectory && VersionManifestLite.isVersionScratchFileName(entry.name) {
            if try await isCurrentVersionScratch(entry.path) {
                return true
            }
        }
        return false
    }

    private func isCurrentVersionScratch(_ path: String) async throws -> Bool {
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.versionFileName)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await client.download(remotePath: path, localURL: localURL)
        } catch {
            let category = RemoteFaultLite.classify(error)
            if category == .notFound { return false }
            throw Self.probeFault(from: error, category: category)
        }
        guard let data = try? Data(contentsOf: localURL),
              let manifest = try? VersionManifestLite.decode(data),
              VersionManifestLite.isCurrent(manifest) else {
            return false
        }
        return true
    }

    // MARK: - Repo directory inspection

    private struct RepoDirState {
        var hasMonthSqlite = false
        var hasUnknownChild = false
        var hasVersionFile = false
        var repoDirectoryEntries: [RemoteStorageEntry]?
        var monthsDirectoryEntries: [RemoteStorageEntry]?

        func probe(decision: RepoFormatDecision) -> RepoFormatProbe {
            RepoFormatProbe(
                decision: decision,
                repoDirectoryEntries: repoDirectoryEntries,
                monthsDirectoryEntries: monthsDirectoryEntries
            )
        }
    }

    private func inspectRepoDirectory(
        scanMonths: Bool = true,
        ignoreMonthsListFault: Bool = false,
        entries listedEntries: [RemoteStorageEntry]? = nil
    ) async throws -> RepoDirState {
        var state = RepoDirState()
        let entries: [RemoteStorageEntry]
        if let listedEntries {
            entries = listedEntries
        } else {
            do {
                entries = try await client.list(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
            } catch {
                let category = RemoteFaultLite.classify(error)
                if category == .notFound { return state }   // raced away between LIST calls
                throw Self.probeFault(from: error, category: category)
            }
        }
        state.repoDirectoryEntries = entries

        var monthsDirPresent = false
        for entry in entries {
            if entry.isDirectory, entry.name == RepoLayoutLite.monthsDirectoryName {
                monthsDirPresent = true
                continue
            }
            if entry.isDirectory, entry.name == RepoLayoutLite.locksDirectoryName {
                continue
            }
            if !entry.isDirectory, entry.name == RepoLayoutLite.versionFileName {
                state.hasVersionFile = true
                continue
            }
            if !entry.isDirectory, entry.name == RepoLayoutLite.legacyV1PrunePendingFileName {
                continue
            }
            if !entry.isDirectory, VersionManifestLite.isVersionScratchFileName(entry.name) {
                continue
            }
            if !entry.isDirectory, RepoLayoutLite.isMoveProbeScratchFileName(entry.name) {
                // Transient MOVE-independence probe scratch, not foreign control state: a stray one must not
                // route an uncommitted repo to .damaged and block fresh-init / V1-migration retry.
                continue
            }
            if !entry.isDirectory, Self.isNoiseFileName(entry.name) {
                continue
            }
            state.hasUnknownChild = true
        }
        if monthsDirPresent && scanMonths {
            do {
                let monthsEntries = try await listMonthsDirectoryEntries()
                state.monthsDirectoryEntries = monthsEntries
                for entry in monthsEntries {
                    let occupiesMonthSlot = entry.name.hasSuffix(".\(RepoLayoutLite.monthFileExtension)")
                        && !Self.isNoiseFileName(entry.name)
                    if entry.isDirectory {
                        // A directory occupying a month-manifest slot ("<YYYY-MM>.sqlite/") is damaged/foreign
                        // control state, not empty space: a month file can't be minted/flushed over it (the
                        // V1→Lite path already fails closed on the same shape).
                        if occupiesMonthSlot { state.hasUnknownChild = true }
                        continue
                    }
                    if RepoLayoutLite.month(fromFilename: entry.name) != nil {
                        state.hasMonthSqlite = true
                    } else if occupiesMonthSlot {
                        // Non-month sqlite control files are not empty space.
                        state.hasUnknownChild = true
                    }
                }
            } catch RepoFormatRouterError.probeFault(let category, _) where ignoreMonthsListFault && category != .cancelled {
                state.monthsDirectoryEntries = nil
            }
        }
        return state
    }

    private func listMonthsDirectoryEntries() async throws -> [RemoteStorageEntry] {
        do {
            return try await client.list(path: RepoLayoutLite.monthsDirectoryPath(basePath: basePath))
        } catch {
            let category = RemoteFaultLite.classify(error)
            if category == .notFound { return [] }
            throw Self.probeFault(from: error, category: category)
        }
    }

    // MARK: - V1 manifests

    // Single-pass V1 evidence via the shared scanner; reuses `baseEntries` so the base directory is not
    // re-listed. A readable file manifest routes .v1Migrate; a directory-only candidate is damaged control
    // state (caller fails closed); nothing is fresh. A non-notFound fault surfaces as a probe fault so the
    // caller never mistakes an interrupted scan for "no V1 data".
    private func v1Evidence(baseEntries: [RemoteStorageEntry]) async throws -> V1ManifestScanner.V1Evidence {
        do {
            return try await V1ManifestScanner(client: client, basePath: basePath)
                .v1Evidence(baseEntries: baseEntries)
        } catch {
            throw Self.probeFault(from: error)
        }
    }

    private static func probeFault(from error: Error, category: RemoteFaultLite.Category? = nil) -> RepoFormatRouterError {
        let category = category ?? RemoteFaultLite.classify(error)
        return .probeFault(category, detail: probeFaultDetail(for: error, category: category))
    }

    private static func probeFaultDetail(for error: Error, category: RemoteFaultLite.Category) -> String? {
        guard category == .terminal else { return nil }
        let detail = neutralErrorDescription(error)
            .components(separatedBy: .whitespacesAndNewlines)
            .filter { !$0.isEmpty }
            .joined(separator: " ")
        return detail.isEmpty ? nil : detail
    }

    private static func neutralErrorDescription(_ error: Error) -> String {
        if let storage = error as? RemoteStorageClientError {
            switch storage {
            case .underlying(let inner):
                return neutralErrorDescription(inner)
            default:
                return storage.errorDescription ?? error.localizedDescription
            }
        }
        return error.localizedDescription
    }
}
