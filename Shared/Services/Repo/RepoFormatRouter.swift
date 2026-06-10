import Foundation

// Dormant Repo V2 format router (Stage B, Step 4). Decides what an existing remote looks like before
// any write: a committed Lite repo, a legacy V1 tree to migrate, a damaged/foreign tree, or empty
// space safe to initialize. Nothing in production wires this yet — BackupRunPreparation still uses
// RemoteFormatCompatibilityService. It fails closed: a probe that can't be resolved throws rather than
// guessing `.fresh`, so an offline/blinking backend never reads as "empty, safe to overwrite".
nonisolated enum RepoFormatDecision: Equatable, Sendable {
    case current          // committed version.json: format 2 + lite layout
    case fresh            // nothing here (or a half-created marker dir); safe to initialize
    case v1Migrate        // legacy V1 month manifests present, no committed version
    case damaged          // Lite month data with no committed version
    case malformedVersion // version.json present but unreadable/incomplete: owned repair route, not generic damage
    case unsupported      // future/foreign committed format, layout mismatch, or dev/v2 marker dirs
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
            case .valid(let manifest):
                // Committed version is the only format commit point: trust it and never scan V1.
                return VersionManifestLite.isCurrent(manifest) ? .current : .unsupported
            case .malformed:
                // An unreadable/incomplete version.json is the format marker itself failing, not foreign
                // data: route to the owned repair path rather than terminating as generic damage.
                return .malformedVersion
            case .missing:
                break
            }

            let repoState = try await inspectRepoDirectory()
            if repoState.hasDevMarker {
                return .unsupported
            }
            if try await hasV1Manifests(baseEntries: baseEntries) {
                return .v1Migrate
            }
            if repoState.hasMonthSqlite {
                return .damaged
            }
            return .fresh
        }

        if try await hasV1Manifests(baseEntries: baseEntries) {
            return .v1Migrate
        }
        return .fresh
    }

    // MARK: - Version

    private enum VersionRead {
        case valid(WatermelonRemoteVersionManifest)
        case missing
        case malformed
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

        // A committed version requires the format marker to decode; anything else is corrupt, not absent.
        guard let data = try? Data(contentsOf: localURL),
              let manifest = try? VersionManifestLite.decode(data),
              manifest.formatVersion != nil else {
            return .malformed
        }
        return .valid(manifest)
    }

    // MARK: - Repo directory inspection

    private struct RepoDirState {
        var hasDevMarker = false
        var hasMonthSqlite = false
    }

    private func inspectRepoDirectory() async throws -> RepoDirState {
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
            }
            if entry.isDirectory, entry.name == RepoLayoutLite.monthsDirectoryName {
                monthsDirPresent = true
            }
        }
        if monthsDirPresent {
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

    // Mirrors RemoteIndexSyncService's scan shape (base year dirs → month dirs → manifest metadata)
    // but short-circuits on the first manifest found. A non-notFound fault anywhere throws so the
    // caller never mistakes an interrupted scan for "no V1 data".
    private func hasV1Manifests(baseEntries: [RemoteStorageEntry]) async throws -> Bool {
        let yearEntries = baseEntries.filter { $0.isDirectory && Self.parseYear($0.name) != nil }
        for yearEntry in yearEntries {
            let monthEntries: [RemoteStorageEntry]
            do {
                monthEntries = try await client.list(path: yearEntry.path)
            } catch {
                let category = RemoteFaultLite.classify(error)
                if category == .notFound { continue }
                throw RepoFormatRouterError.probeFault(category)
            }

            for monthEntry in monthEntries where monthEntry.isDirectory && Self.parseMonth(monthEntry.name) != nil {
                let manifestPath = RemotePathBuilder.absolutePath(
                    basePath: basePath,
                    remoteRelativePath: "\(yearEntry.name)/\(monthEntry.name)/\(MonthManifestStore.manifestFileName)"
                )
                do {
                    if let metadata = try await client.metadata(path: manifestPath), !metadata.isDirectory {
                        return true
                    }
                } catch {
                    let category = RemoteFaultLite.classify(error)
                    if category == .notFound { continue }
                    throw RepoFormatRouterError.probeFault(category)
                }
            }
        }
        return false
    }

    private static func parseYear(_ value: String) -> Int? {
        guard value.count == 4, let number = Int(value), number >= 1900 else { return nil }
        return number
    }

    private static func parseMonth(_ value: String) -> Int? {
        guard value.count == 2, let number = Int(value), (1 ... 12).contains(number) else { return nil }
        return number
    }
}
