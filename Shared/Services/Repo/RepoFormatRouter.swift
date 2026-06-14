import Foundation

// Repo V2 (Lite) format router. Decides what an existing remote looks like before any write: a committed
// Lite repo, a legacy V1 tree to migrate, a damaged/foreign tree, or empty space safe to initialize.
// It fails closed: a probe that can't be resolved throws rather than guessing `.fresh`.
nonisolated enum RepoFormatDecision: Equatable, Sendable {
    case current          // committed version.json: format 2 + lite layout
    case fresh            // nothing here (or a half-created marker dir); safe to initialize
    case v1Migrate        // legacy V1 month manifests present, no committed version
    case damaged          // Lite month data with no committed version
    case malformedVersion // canonical version missing, but current version scratch can be recovered
    case unsupported(minAppVersion: String? = nil) // future/foreign committed format, layout mismatch, or dev/v2 marker dirs
}

private extension RepoFormatDecision {
    var isUnsupported: Bool {
        if case .unsupported = self { return true }
        return false
    }
}

enum RepoFormatRouterError: Error, Equatable {
    case probeFault(RemoteFaultLite.Category)
}

struct RepoFormatProbe: Sendable {
    let decision: RepoFormatDecision
    let repoDirectoryEntries: [RemoteStorageEntry]?
    let monthsDirectoryEntries: [RemoteStorageEntry]?
}

struct RepoFormatRouter: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    // Subdirectories from the abandoned CRDT/commit-log V2 design. Their presence means a writer we
    // can't interpret touched this repo, so fail closed rather than risk corrupting it.
    private static let devMarkerNames: Set<String> = ["commits", "snapshots"]

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
            throw RepoFormatRouterError.probeFault(category)
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
            switch try await readVersion() {
            case .current:
                let repoState = try await inspectRepoDirectory(
                    scanMonths: collectCurrentMonthsListing,
                    ignoreMonthsListFault: true
                )
                if repoState.hasDevMarker {
                    return repoState.probe(decision: .unsupported())
                }
                // Committed version is the only format commit point: trust it and never scan V1.
                return repoState.probe(decision: .current)
            case .unsupported(let minAppVersion):
                return RepoFormatProbe(
                    decision: .unsupported(minAppVersion: minAppVersion),
                    repoDirectoryEntries: nil,
                    monthsDirectoryEntries: nil
                )
            case .damaged:
                let uncommitted = try await classifyUncommittedRepo(
                    baseEntries: baseEntries,
                    preferLiteDamageOverV1: true
                )
                switch uncommitted {
                case let probe where probe.decision.isUnsupported:
                    return probe
                default:
                    return RepoFormatProbe(
                        decision: .damaged,
                        repoDirectoryEntries: uncommitted.repoDirectoryEntries,
                        monthsDirectoryEntries: uncommitted.monthsDirectoryEntries
                    )
                }
            case .missing:
                break
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
        preferLiteDamageOverV1: Bool
    ) async throws -> RepoFormatProbe {
        let repoState = try await inspectRepoDirectory()
        if repoState.hasDevMarker {
            return repoState.probe(decision: .unsupported())
        }
        if repoState.hasMonthSqlite, try await hasRecoverableVersionScratch() {
            return repoState.probe(decision: .malformedVersion)
        }
        if preferLiteDamageOverV1, repoState.hasMonthSqlite {
            return repoState.probe(decision: .damaged)
        }
        if preferLiteDamageOverV1, repoState.hasUnknownChild {
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
        if repoState.hasUnknownChild {
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
            throw RepoFormatRouterError.probeFault(category)
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

    private func hasRecoverableVersionScratch() async throws -> Bool {
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
        } catch {
            let category = RemoteFaultLite.classify(error)
            if category == .notFound { return false }
            throw RepoFormatRouterError.probeFault(category)
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
            throw RepoFormatRouterError.probeFault(category)
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
        var hasDevMarker = false
        var hasMonthSqlite = false
        var hasUnknownChild = false
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
        ignoreMonthsListFault: Bool = false
    ) async throws -> RepoDirState {
        var state = RepoDirState()
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
        } catch {
            let category = RemoteFaultLite.classify(error)
            if category == .notFound { return state }   // raced away between LIST calls
            throw RepoFormatRouterError.probeFault(category)
        }
        state.repoDirectoryEntries = entries

        var monthsDirPresent = false
        for entry in entries {
            if Self.devMarkerNames.contains(entry.name) {
                state.hasDevMarker = true
                continue
            }
            if entry.isDirectory, entry.name == RepoLayoutLite.monthsDirectoryName {
                monthsDirPresent = true
                continue
            }
            if entry.isDirectory, entry.name == RepoLayoutLite.locksDirectoryName {
                continue
            }
            if !entry.isDirectory, entry.name == RepoLayoutLite.versionFileName {
                continue
            }
            if !entry.isDirectory, VersionManifestLite.isVersionScratchFileName(entry.name) {
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
            } catch RepoFormatRouterError.probeFault(let category) where ignoreMonthsListFault && category != .cancelled {
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
            throw RepoFormatRouterError.probeFault(category)
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
            throw RepoFormatRouterError.probeFault(RemoteFaultLite.classify(error))
        }
    }
}
