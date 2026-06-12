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

enum RepoFormatRouterError: Error, Equatable {
    case probeFault(RemoteFaultLite.Category)
}

struct RepoFormatRouter: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    // Subdirectories from the abandoned CRDT/commit-log V2 design. Their presence means a writer we
    // can't interpret touched this repo, so fail closed rather than risk corrupting it.
    private static let devMarkerNames: Set<String> = ["commits", "snapshots"]

    func classify() async throws -> RepoFormatDecision {
        let normalizedBase = RemotePathBuilder.normalizePath(basePath)
        let baseEntries: [RemoteStorageEntry]
        do {
            baseEntries = try await client.list(path: normalizedBase)
        } catch {
            let category = RemoteFaultLite.classify(error)
            if category == .notFound { return .fresh }   // base path absent → nothing here yet
            throw RepoFormatRouterError.probeFault(category)
        }

        let repoDirPresent = baseEntries.contains {
            $0.isDirectory && $0.name == RepoLayoutLite.repoDirectoryName
        }

        if repoDirPresent {
            switch try await readVersion() {
            case .current:
                if try await inspectRepoDirectory(scanMonths: false).hasDevMarker {
                    return .unsupported()
                }
                // Committed version is the only format commit point: trust it and never scan V1.
                return .current
            case .unsupported(let minAppVersion):
                return .unsupported(minAppVersion: minAppVersion)
            case .damaged:
                let uncommitted = try await classifyUncommittedRepo(
                    baseEntries: baseEntries,
                    preferLiteDamageOverV1: true
                )
                switch uncommitted {
                case .unsupported:
                    return uncommitted
                default:
                    return .damaged
                }
            case .missing:
                break
            }

            return try await classifyUncommittedRepo(
                baseEntries: baseEntries,
                preferLiteDamageOverV1: false
            )
        }

        if try await hasV1Manifests(baseEntries: baseEntries) {
            return .v1Migrate
        }
        return .fresh
    }

    private func classifyUncommittedRepo(
        baseEntries: [RemoteStorageEntry],
        preferLiteDamageOverV1: Bool
    ) async throws -> RepoFormatDecision {
        let repoState = try await inspectRepoDirectory()
        if repoState.hasDevMarker {
            return .unsupported()
        }
        if repoState.hasMonthSqlite, try await hasRecoverableVersionScratch() {
            return .malformedVersion
        }
        if preferLiteDamageOverV1, repoState.hasMonthSqlite {
            return .damaged
        }
        if preferLiteDamageOverV1, repoState.hasUnknownChild {
            return .damaged
        }
        if try await hasV1Manifests(baseEntries: baseEntries) {
            return .v1Migrate
        }
        if repoState.hasMonthSqlite {
            return .damaged
        }
        if repoState.hasUnknownChild {
            return .damaged
        }
        return .fresh
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
    }

    private func inspectRepoDirectory(scanMonths: Bool = true) async throws -> RepoDirState {
        var state = RepoDirState()
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
        } catch {
            let category = RemoteFaultLite.classify(error)
            if category == .notFound { return state }   // raced away between LIST calls
            throw RepoFormatRouterError.probeFault(category)
        }

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
            state.hasUnknownChild = true
        }
        if monthsDirPresent && scanMonths {
            state.hasMonthSqlite = try await monthsDirectoryHasSqlite()
        }
        return state
    }

    private func monthsDirectoryHasSqlite() async throws -> Bool {
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: RepoLayoutLite.monthsDirectoryPath(basePath: basePath))
        } catch {
            let category = RemoteFaultLite.classify(error)
            if category == .notFound { return false }
            throw RepoFormatRouterError.probeFault(category)
        }
        let suffix = ".\(RepoLayoutLite.monthFileExtension)"
        return entries.contains { !$0.isDirectory && $0.name.hasSuffix(suffix) }
    }

    // MARK: - V1 manifests

    // Short-circuits on the first manifest found via the shared scanner; reuses `baseEntries` so the base
    // directory is not re-listed. A non-notFound fault surfaces as a probe fault so the caller never
    // mistakes an interrupted scan for "no V1 data".
    private func hasV1Manifests(baseEntries: [RemoteStorageEntry]) async throws -> Bool {
        do {
            return try await V1ManifestScanner(client: client, basePath: basePath)
                .containsManifest(baseEntries: baseEntries)
        } catch {
            throw RepoFormatRouterError.probeFault(RemoteFaultLite.classify(error))
        }
    }
}
