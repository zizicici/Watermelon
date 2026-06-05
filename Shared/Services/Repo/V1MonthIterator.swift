import Foundation

/// Shared `base → year → month` traversal for V1 layout discovery. Remote-format V1 detection,
/// `V1MigrationService.scanV1Months`, and the V1 sync engine's digest scan each used to hand-roll the
/// same year/month filtering and per-level listing; centralising it keeps the `YYYY/MM` domain and the
/// list-failure policy from drifting between admission, migration, and sync.
nonisolated enum V1MonthIterator {
    /// How a directory `list` failure is handled while traversing.
    enum ListFailurePolicy: Sendable {
        /// Admission/detection: a vanished directory (concurrent removal / eventual consistency) is skipped,
        /// cancellation normalizes to `CancellationError`, and every other error propagates.
        case skipMissing
        /// Migration/sync scan: every list failure propagates raw so a not-found can't be misread as
        /// "no V1 month" and let a later sweep delete an unscanned manifest.
        case propagate
    }

    enum Order: Sendable {
        case ascending
        case descending
        /// Preserve the backend's listing order (post-filter).
        case asListed
    }

    enum Step: Sendable {
        case `continue`
        case stop
    }

    struct Options: Sendable {
        var listFailurePolicy: ListFailurePolicy
        var yearOrder: Order
        var monthOrder: Order
        /// Lower bound on the four-digit year directory (the digest scan rejects pre-1900 dirs).
        var minYear: Int

        init(
            listFailurePolicy: ListFailurePolicy,
            yearOrder: Order = .asListed,
            monthOrder: Order = .asListed,
            minYear: Int = 0
        ) {
            self.listFailurePolicy = listFailurePolicy
            self.yearOrder = yearOrder
            self.monthOrder = monthOrder
            self.minYear = minYear
        }
    }

    /// Walks `basePath → <YYYY> → <MM>` and invokes `body` once per in-domain month directory. Return
    /// `.stop` from `body` to end the walk early (used by detection's first-hit short-circuit).
    /// `baseEntries` lets a caller that already listed the base pass it in to avoid a redundant level-0 list.
    static func forEachMonth(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        options: Options,
        baseEntries: [RemoteStorageEntry]? = nil,
        body: (_ year: Int, _ month: Int, _ monthPath: String) async throws -> Step
    ) async throws {
        let entries: [RemoteStorageEntry]
        if let baseEntries {
            entries = baseEntries
        } else {
            entries = try await listDirectory(client: client, path: basePath, policy: options.listFailurePolicy) ?? []
        }

        let years = ordered(
            entries.filter { $0.isDirectory && parseYear($0.name, minYear: options.minYear) != nil },
            options.yearOrder
        )
        for yearEntry in years {
            guard let year = parseYear(yearEntry.name, minYear: options.minYear) else { continue }
            let yearPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: yearEntry.name)
            guard let monthEntries = try await listDirectory(client: client, path: yearPath, policy: options.listFailurePolicy) else {
                continue
            }
            let months = ordered(
                monthEntries.filter { $0.isDirectory && parseMonth($0.name) != nil },
                options.monthOrder
            )
            for monthEntry in months {
                guard let month = parseMonth(monthEntry.name) else { continue }
                let monthPath = RemotePathBuilder.absolutePath(basePath: yearPath, remoteRelativePath: monthEntry.name)
                if try await body(year, month, monthPath) == .stop { return }
            }
        }
    }

    /// Lists `monthPath` honoring `policy` and reports whether it holds a V1 month manifest file.
    /// Under `.skipMissing` a vanished month directory contributes `false`; under `.propagate` the
    /// list error surfaces.
    static func monthContainsManifest(
        client: any RemoteStorageClientProtocol,
        monthPath: String,
        listFailurePolicy: ListFailurePolicy
    ) async throws -> Bool {
        guard let contents = try await listDirectory(client: client, path: monthPath, policy: listFailurePolicy) else {
            return false
        }
        return contents.contains { !$0.isDirectory && $0.name == MonthManifestStore.manifestFileName }
    }

    private static func listDirectory(
        client: any RemoteStorageClientProtocol,
        path: String,
        policy: ListFailurePolicy
    ) async throws -> [RemoteStorageEntry]? {
        switch policy {
        case .propagate:
            return try await client.list(path: path)
        case .skipMissing:
            do {
                return try await client.list(path: path)
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                if isStorageNotFoundError(error) { return nil }
                throw error
            }
        }
    }

    private static func ordered(_ entries: [RemoteStorageEntry], _ order: Order) -> [RemoteStorageEntry] {
        switch order {
        case .asListed: return entries
        case .ascending: return entries.sorted { $0.name < $1.name }
        case .descending: return entries.sorted { $0.name > $1.name }
        }
    }

    private static func parseYear(_ name: String, minYear: Int) -> Int? {
        guard name.count == 4, isAllASCIIDigits(name), let year = Int(name), year >= minYear else { return nil }
        return year
    }

    private static func parseMonth(_ name: String) -> Int? {
        guard name.count == 2, isAllASCIIDigits(name), let month = Int(name), (1...12).contains(month) else { return nil }
        return month
    }

    private static func isAllASCIIDigits(_ value: String) -> Bool {
        value.allSatisfy { $0 >= "0" && $0 <= "9" }
    }
}
