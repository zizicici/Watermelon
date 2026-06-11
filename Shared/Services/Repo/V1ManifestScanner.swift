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

    static func parseYear(_ value: String) -> Int? {
        guard value.count == 4, let number = Int(value), number >= 1900 else { return nil }
        return number
    }

    static func parseMonth(_ value: String) -> Int? {
        guard value.count == 2, let number = Int(value), (1 ... 12).contains(number) else { return nil }
        return number
    }

    // Full deterministic scan. `baseEntries`, when supplied, is a base listing the caller already holds (the
    // router) so the base directory is not re-listed; otherwise the scanner lists the normalized base itself
    // and treats an absent base as zero months. `checkCancellation` runs before each remote call.
    func scan(
        baseEntries: [RemoteStorageEntry]? = nil,
        missingBaseIsEmpty: Bool = false,
        checkCancellation: (() throws -> Void)? = nil
    ) async throws -> [Manifest] {
        var result: [Manifest] = []
        try await traverse(
            baseEntries: baseEntries,
            missingBaseIsEmpty: missingBaseIsEmpty,
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
            checkCancellation: nil
        ) { _ in
            found = true
            return false
        }
        return found
    }

    // MARK: - Traversal

    // Visits every present manifest in deterministic order; a false `onManifest` return stops the walk early.
    private func traverse(
        baseEntries: [RemoteStorageEntry]?,
        missingBaseIsEmpty: Bool,
        checkCancellation: (() throws -> Void)?,
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
                guard let metadata, !metadata.isDirectory else { continue }
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
