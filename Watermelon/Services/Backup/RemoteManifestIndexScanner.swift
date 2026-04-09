import Foundation

final class RemoteManifestIndexScanner: Sendable {
    func scanManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        cancellationController: BackupCancellationController? = nil
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        let normalizedBasePath = RemotePathBuilder.normalizePath(basePath)
        try cancellationController?.throwIfCancelled()

        let yearEntries = try await client.list(path: normalizedBasePath)
            .filter { $0.isDirectory }
            .filter { Self.parseYear($0.name) != nil }
            .sorted { $0.name < $1.name }

        var digests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        digests.reserveCapacity(yearEntries.count * 12)

        for yearEntry in yearEntries {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            guard let year = Self.parseYear(yearEntry.name) else { continue }

            let monthEntries = try await client.list(path: yearEntry.path)
                .filter { $0.isDirectory }
                .filter { Self.parseMonth($0.name) != nil }
                .sorted { $0.name < $1.name }

            for monthEntry in monthEntries {
                try cancellationController?.throwIfCancelled()
                try Task.checkCancellation()
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
                let modifiedNs = manifestEntry.modificationDate?.nanosecondsSinceEpoch
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

}

