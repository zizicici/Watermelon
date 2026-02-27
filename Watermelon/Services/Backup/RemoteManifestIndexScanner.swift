import Foundation

final class RemoteManifestIndexScanner {
    func scanManifestDigests(
        client: SMBClientProtocol,
        basePath: String
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        let normalizedBasePath = RemotePathBuilder.normalizePath(basePath)

        let yearEntries = try await client.list(path: normalizedBasePath)
            .filter { $0.isDirectory }
            .filter { Self.parseYear($0.name) != nil }
            .sorted { $0.name < $1.name }

        var digests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        digests.reserveCapacity(yearEntries.count * 12)

        for yearEntry in yearEntries {
            guard let year = Self.parseYear(yearEntry.name) else { continue }

            let monthEntries = try await client.list(path: yearEntry.path)
                .filter { $0.isDirectory }
                .filter { Self.parseMonth($0.name) != nil }
                .sorted { $0.name < $1.name }

            for monthEntry in monthEntries {
                guard let month = Self.parseMonth(monthEntry.name) else { continue }
                let manifestPath = RemotePathBuilder.absolutePath(
                    basePath: normalizedBasePath,
                    remoteRelativePath: "\(yearEntry.name)/\(monthEntry.name)/\(MonthManifestStore.manifestFileName)"
                )
                guard let manifestEntry = try await client.metadata(path: manifestPath),
                      manifestEntry.isDirectory == false else {
                    continue
                }

                let monthKey = LibraryMonthKey(year: year, month: month)
                let modifiedNs = Self.nanosecondsSinceEpoch(manifestEntry.modificationDate)
                digests[monthKey] = RemoteMonthManifestDigest(
                    month: monthKey,
                    manifestSize: manifestEntry.size,
                    manifestModifiedAtNs: modifiedNs
                )
            }
        }

        return digests
    }

    private static func parseYear(_ value: String) -> Int? {
        guard value.count == 4, let number = Int(value), number >= 1900 else { return nil }
        return number
    }

    private static func parseMonth(_ value: String) -> Int? {
        guard value.count == 2, let number = Int(value), (1 ... 12).contains(number) else { return nil }
        return number
    }

    private static func nanosecondsSinceEpoch(_ date: Date?) -> Int64? {
        guard let date else { return nil }
        return Int64((date.timeIntervalSince1970 * 1_000_000_000).rounded())
    }
}
