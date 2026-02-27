import Foundation

struct RemoteLibraryAggregate {
    let totalCount: Int
    let countsByMonth: [String: Int]
}

final class RemoteLibraryScanner {
    func scanYearMonthTree(
        client: SMBClientProtocol,
        basePath: String
    ) async throws -> RemoteLibrarySnapshot {
        let normalizedBasePath = RemotePathBuilder.normalizePath(basePath)
        try await client.createDirectory(path: normalizedBasePath)

        let yearEntries = try await client.list(path: normalizedBasePath)
            .filter { $0.isDirectory }
            .filter { Self.parseYear($0.name) != nil }
            .sorted { $0.name < $1.name }

        var allItems: [RemoteManifestResource] = []
        var allAssets: [RemoteManifestAsset] = []
        var allLinks: [RemoteAssetResourceLink] = []

        for yearEntry in yearEntries {
            guard let year = Self.parseYear(yearEntry.name) else { continue }
            let monthEntries = try await client.list(path: yearEntry.path)
                .filter { $0.isDirectory }
                .filter { Self.parseMonth($0.name) != nil }
                .sorted { $0.name < $1.name }

            for monthEntry in monthEntries {
                guard let month = Self.parseMonth(monthEntry.name) else { continue }
                let store = try await MonthManifestStore.loadOrCreate(
                    client: client,
                    basePath: normalizedBasePath,
                    year: year,
                    month: month
                )
                let monthItems = store.allItems()
                let monthAssets = store.allAssets()
                allItems.append(contentsOf: monthItems)
                allAssets.append(contentsOf: monthAssets)
                for asset in monthAssets {
                    allLinks.append(contentsOf: store.links(forAssetFingerprint: asset.assetFingerprint))
                }
            }
        }

        allItems.sort { lhs, rhs in
            if lhs.year != rhs.year { return lhs.year < rhs.year }
            if lhs.month != rhs.month { return lhs.month < rhs.month }
            if lhs.creationDateNs != rhs.creationDateNs {
                return (lhs.creationDateNs ?? lhs.backedUpAtNs) < (rhs.creationDateNs ?? rhs.backedUpAtNs)
            }
            return lhs.fileName < rhs.fileName
        }

        allAssets.sort { lhs, rhs in
            if lhs.year != rhs.year { return lhs.year < rhs.year }
            if lhs.month != rhs.month { return lhs.month < rhs.month }
            if lhs.creationDateNs != rhs.creationDateNs {
                return (lhs.creationDateNs ?? lhs.backedUpAtNs) < (rhs.creationDateNs ?? rhs.backedUpAtNs)
            }
            return lhs.assetFingerprintHex < rhs.assetFingerprintHex
        }

        allLinks.sort { lhs, rhs in
            if lhs.year != rhs.year { return lhs.year < rhs.year }
            if lhs.month != rhs.month { return lhs.month < rhs.month }
            if lhs.assetFingerprint != rhs.assetFingerprint {
                return lhs.assetFingerprint.lexicographicallyPrecedes(rhs.assetFingerprint)
            }
            if lhs.role != rhs.role { return lhs.role < rhs.role }
            if lhs.slot != rhs.slot { return lhs.slot < rhs.slot }
            return lhs.resourceHash.lexicographicallyPrecedes(rhs.resourceHash)
        }

        return RemoteLibrarySnapshot(resources: allItems, assets: allAssets, assetResourceLinks: allLinks)
    }

    func aggregateRemoteCounts(snapshot: RemoteLibrarySnapshot) -> RemoteLibraryAggregate {
        RemoteLibraryAggregate(totalCount: snapshot.totalCount, countsByMonth: snapshot.countsByMonth)
    }

    private static func parseYear(_ value: String) -> Int? {
        guard value.count == 4, let number = Int(value), number >= 1900 else { return nil }
        return number
    }

    private static func parseMonth(_ value: String) -> Int? {
        guard value.count == 2, let number = Int(value), (1...12).contains(number) else { return nil }
        return number
    }
}
