import Foundation

// Shared legacy-V1 manifest scanner. Walks <base>/<YYYY>/<MM>/.watermelon_manifest.sqlite in one
// deterministic order — year dirs sorted by name, then month dirs sorted by name — probing each candidate
// manifest's metadata. Strict on transport faults: a non-notFound list/metadata fault surfaces so an
// interrupted scan is never read as "no V1 data". notFound is intentional absence — an absent base, a
// vanished year dir, or a missing candidate manifest is skipped rather than thrown. Centralizes the
// year/month parsing and traversal that cleanup, routing, migration, and remote index sync each duplicated.
struct V1ManifestScanner: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    struct Manifest: Equatable, Sendable {
        let month: LibraryMonthKey
        let manifestPath: String
        let size: Int64
        let modificationDate: Date?
    }

    // What the router's V1 detection found: a readable file manifest (route .v1Migrate), only a directory
    // occupying a canonical V1 manifest slot with no readable manifest (damaged/foreign control state — must
    // fail closed before a Lite version marker commits), or nothing (genuinely fresh).
    enum V1Evidence: Equatable, Sendable {
        case none
        case directoryCandidateOnly
        case validManifest
    }

    static func parseYear(_ value: String) -> Int? {
        // Accept any 4-digit year, matching RepoLayoutLite.parseMonthKey. LibraryMonthKey.from(date:) imposes
        // no lower bound, so the V1 writer can produce <1900/MM months; a floor here would silently orphan
        // that already-backed-up data from V1→Lite migration, router detection, and V1 index sync. ASCII
        // digits only: Int() would accept signed names ("-001") that the Lite layout cannot round-trip.
        guard value.count == 4, isAllASCIIDigits(value), let number = Int(value) else { return nil }
        return number
    }

    static func parseMonth(_ value: String) -> Int? {
        guard value.count == 2, isAllASCIIDigits(value), let number = Int(value), (1 ... 12).contains(number) else { return nil }
        return number
    }

    private static func isAllASCIIDigits(_ value: String) -> Bool {
        value.allSatisfy { $0.isASCII && $0.isNumber }
    }

    // Full deterministic scan. `baseEntries`, when supplied, is a base listing the caller already holds (the
    // router) so the base directory is not re-listed; otherwise the scanner lists the normalized base itself
    // and treats an absent base as zero months. `checkCancellation` runs before each remote call.
    // `failOnDirectoryCandidate` is for the migration (write/commit) plane only: a directory occupying a
    // candidate V1 manifest slot is damaged/foreign control state, so migration fails closed before
    // committing version.json rather than silently dropping the month. Router/read-index callers leave it
    // false and keep skipping the directory.
    func scan(
        baseEntries: [RemoteStorageEntry]? = nil,
        missingBaseIsEmpty: Bool = false,
        failOnDirectoryCandidate: Bool = false,
        checkCancellation: (() throws -> Void)? = nil
    ) async throws -> [Manifest] {
        var result: [Manifest] = []
        try await traverse(
            baseEntries: baseEntries,
            missingBaseIsEmpty: missingBaseIsEmpty,
            failOnDirectoryCandidate: failOnDirectoryCandidate,
            checkCancellation: checkCancellation
        ) { manifest in
            result.append(manifest)
            return true
        }
        return result
    }

    // Short-circuit existence probe for the router: stops at the first manifest. Same strict fault policy.
    func containsManifest(baseEntries: [RemoteStorageEntry]? = nil, missingBaseIsEmpty: Bool = false) async throws -> Bool {
        var found = false
        try await traverse(
            baseEntries: baseEntries,
            missingBaseIsEmpty: missingBaseIsEmpty,
            failOnDirectoryCandidate: false,
            checkCancellation: nil
        ) { _ in
            found = true
            return false
        }
        return found
    }

    // Single-pass V1 evidence probe for the router. A readable file manifest is decisive (`.validManifest`);
    // a directory occupying a canonical V1 manifest slot with no readable manifest is damaged control state
    // (`.directoryCandidateOnly`). Same strict transport-fault policy as scan/containsManifest.
    func v1Evidence(baseEntries: [RemoteStorageEntry]? = nil, missingBaseIsEmpty: Bool = false) async throws -> V1Evidence {
        var hasValidManifest = false
        var hasDirectoryCandidate = false
        try await traverse(
            baseEntries: baseEntries,
            missingBaseIsEmpty: missingBaseIsEmpty,
            failOnDirectoryCandidate: false,
            checkCancellation: nil,
            onDirectoryCandidate: { hasDirectoryCandidate = true; return true },
            onManifest: { _ in hasValidManifest = true; return false }
        )
        if hasValidManifest { return .validManifest }
        return hasDirectoryCandidate ? .directoryCandidateOnly : .none
    }

    // MARK: - Traversal

    // Visits every present manifest in deterministic order; a false `onManifest` return stops the walk early.
    // `onDirectoryCandidate` (router only) observes a directory at a candidate manifest slot; returning false
    // stops the walk. Read-index/migration callers leave it nil and skip (or, in strict mode, throw on) it.
    private func traverse(
        baseEntries: [RemoteStorageEntry]?,
        missingBaseIsEmpty: Bool,
        failOnDirectoryCandidate: Bool,
        checkCancellation: (() throws -> Void)?,
        onDirectoryCandidate: (() -> Bool)? = nil,
        onManifest: (Manifest) -> Bool
    ) async throws {
        let normalizedBase = RemotePathBuilder.normalizePath(basePath)

        try checkCancellation?()
        let entries: [RemoteStorageEntry]
        if let baseEntries {
            entries = baseEntries
        } else {
            do {
                entries = try await client.list(path: normalizedBase)
            } catch {
                if missingBaseIsEmpty, RemoteFaultLite.classify(error) == .notFound { return }
                throw error
            }
        }

        let yearEntries = entries
            .filter { $0.isDirectory && Self.parseYear($0.name) != nil }
            .sorted { $0.name < $1.name }

        for yearEntry in yearEntries {
            guard let year = Self.parseYear(yearEntry.name) else { continue }
            try checkCancellation?()
            let monthEntries: [RemoteStorageEntry]
            do {
                monthEntries = try await client.list(path: yearEntry.path)
            } catch {
                if RemoteFaultLite.classify(error) == .notFound { continue }
                throw error
            }

            let sortedMonths = monthEntries
                .filter { $0.isDirectory && Self.parseMonth($0.name) != nil }
                .sorted { $0.name < $1.name }
            for monthEntry in sortedMonths {
                guard let month = Self.parseMonth(monthEntry.name) else { continue }
                try checkCancellation?()
                let manifestPath = RemotePathBuilder.absolutePath(
                    basePath: normalizedBase,
                    remoteRelativePath: "\(yearEntry.name)/\(monthEntry.name)/\(MonthManifestStore.manifestFileName)"
                )
                let metadata: RemoteStorageEntry?
                do {
                    metadata = try await client.metadata(path: manifestPath)
                } catch {
                    if RemoteFaultLite.classify(error) == .notFound { continue }
                    throw error
                }
                guard let metadata else { continue }
                if metadata.isDirectory {
                    // A directory occupying the V1 manifest slot is damaged/foreign control state. Migration
                    // fails closed before committing version.json; the router observes it (onDirectoryCandidate)
                    // so a directory-only base cannot route .fresh and commit a Lite marker over it; the
                    // read-index caller leaves both hooks unset and keeps skipping it.
                    if failOnDirectoryCandidate {
                        throw LiteRepoError.v1MonthManifestUnreadable(
                            month: LibraryMonthKey(year: year, month: month).text
                        )
                    }
                    if let onDirectoryCandidate, !onDirectoryCandidate() { return }
                    continue
                }
                let manifest = Manifest(
                    month: LibraryMonthKey(year: year, month: month),
                    manifestPath: manifestPath,
                    size: metadata.size,
                    modificationDate: metadata.modificationDate
                )
                if !onManifest(manifest) { return }
            }
        }
    }
}
