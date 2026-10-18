import Foundation

final class RemoteManifestSnapshotDiskCache: Sendable {
    struct CachedSnapshot {
        let digests: [LibraryMonthKey: RemoteMonthManifestDigest]
        let months: [RemoteLibraryMonthDelta]
    }

    private struct Payload: Codable {
        let version: Int
        let profileKey: String
        let layout: String
        let digests: [DigestPayload]
        let months: [MonthPayload]
    }

    private struct DigestPayload: Codable {
        let year: Int
        let month: Int
        let manifestSize: Int64
        let manifestModifiedAtMs: Int64?

        init(_ digest: RemoteMonthManifestDigest) {
            year = digest.month.year
            month = digest.month.month
            manifestSize = digest.manifestSize
            manifestModifiedAtMs = digest.manifestModifiedAtMs
        }

        var value: RemoteMonthManifestDigest {
            let key = LibraryMonthKey(year: year, month: month)
            return RemoteMonthManifestDigest(
                month: key,
                manifestSize: manifestSize,
                manifestModifiedAtMs: manifestModifiedAtMs
            )
        }
    }

    private struct MonthPayload: Codable {
        let year: Int
        let month: Int
        let resources: [ResourcePayload]
        let assets: [AssetPayload]
        let assetResourceLinks: [LinkPayload]

        init(_ delta: RemoteLibraryMonthDelta) {
            year = delta.month.year
            month = delta.month.month
            resources = delta.resources.map(ResourcePayload.init)
            assets = delta.assets.map(AssetPayload.init)
            assetResourceLinks = delta.assetResourceLinks.map(LinkPayload.init)
        }

        var value: RemoteLibraryMonthDelta? {
            guard let resources = Self.decodeAll(resources.map(\.value)),
                  let assets = Self.decodeAll(assets.map(\.value)),
                  let links = Self.decodeAll(assetResourceLinks.map(\.value)) else { return nil }
            return RemoteLibraryMonthDelta(
                month: LibraryMonthKey(year: year, month: month),
                resources: resources,
                assets: assets,
                assetResourceLinks: links
            )
        }

        private static func decodeAll<T>(_ values: [T?]) -> [T]? {
            var result: [T] = []
            result.reserveCapacity(values.count)
            for value in values {
                guard let value else { return nil }
                result.append(value)
            }
            return result
        }
    }

    private struct ResourcePayload: Codable {
        let year: Int
        let month: Int
        let fileName: String
        let contentHashHex: String
        let fileSize: Int64
        let resourceType: Int
        let creationDateMs: Int64?
        let backedUpAtMs: Int64

        init(_ resource: RemoteManifestResource) {
            year = resource.year
            month = resource.month
            fileName = resource.fileName
            contentHashHex = resource.contentHash.hexString
            fileSize = resource.fileSize
            resourceType = resource.resourceType
            creationDateMs = resource.creationDateMs
            backedUpAtMs = resource.backedUpAtMs
        }

        var value: RemoteManifestResource? {
            guard let contentHash = Data(hexString: contentHashHex) else { return nil }
            return RemoteManifestResource(
                year: year,
                month: month,
                fileName: fileName,
                contentHash: contentHash,
                fileSize: fileSize,
                resourceType: resourceType,
                creationDateMs: creationDateMs,
                backedUpAtMs: backedUpAtMs
            )
        }
    }

    private struct AssetPayload: Codable {
        let year: Int
        let month: Int
        let assetFingerprintHex: String
        let creationDateMs: Int64?
        let backedUpAtMs: Int64
        let resourceCount: Int
        let totalFileSizeBytes: Int64

        init(_ asset: RemoteManifestAsset) {
            year = asset.year
            month = asset.month
            assetFingerprintHex = asset.assetFingerprint.hexString
            creationDateMs = asset.creationDateMs
            backedUpAtMs = asset.backedUpAtMs
            resourceCount = asset.resourceCount
            totalFileSizeBytes = asset.totalFileSizeBytes
        }

        var value: RemoteManifestAsset? {
            guard let assetFingerprint = Data(hexString: assetFingerprintHex) else { return nil }
            return RemoteManifestAsset(
                year: year,
                month: month,
                assetFingerprint: assetFingerprint,
                creationDateMs: creationDateMs,
                backedUpAtMs: backedUpAtMs,
                resourceCount: resourceCount,
                totalFileSizeBytes: totalFileSizeBytes
            )
        }
    }

    private struct LinkPayload: Codable {
        let year: Int
        let month: Int
        let assetFingerprintHex: String
        let resourceHashHex: String
        let role: Int
        let slot: Int

        init(_ link: RemoteAssetResourceLink) {
            year = link.year
            month = link.month
            assetFingerprintHex = link.assetFingerprint.hexString
            resourceHashHex = link.resourceHash.hexString
            role = link.role
            slot = link.slot
        }

        var value: RemoteAssetResourceLink? {
            guard let assetFingerprint = Data(hexString: assetFingerprintHex),
                  let resourceHash = Data(hexString: resourceHashHex) else { return nil }
            return RemoteAssetResourceLink(
                year: year,
                month: month,
                assetFingerprint: assetFingerprint,
                resourceHash: resourceHash,
                role: role,
                slot: slot
            )
        }
    }

    private static let currentVersion = 2
    private let directory: URL?

    init(directory: URL? = nil) {
        self.directory = directory
    }

    func load(
        profileKey: String,
        layout: MonthManifestStore.ManifestLayout
    ) -> CachedSnapshot? {
        let url = cacheURL(profileKey: profileKey)
        guard let data = try? Data(contentsOf: url),
              let payload = try? JSONDecoder().decode(Payload.self, from: data),
              payload.version == Self.currentVersion,
              payload.profileKey == profileKey,
              payload.layout == Self.layoutIdentifier(layout) else { return nil }
        var digests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        digests.reserveCapacity(payload.digests.count)
        for encoded in payload.digests {
            let digest = encoded.value
            digests[digest.month] = digest
        }
        var months: [RemoteLibraryMonthDelta] = []
        months.reserveCapacity(payload.months.count)
        for encoded in payload.months {
            guard let value = encoded.value else { return nil }
            months.append(value)
        }
        return CachedSnapshot(digests: digests, months: months)
    }

    func save(
        profileKey: String,
        layout: MonthManifestStore.ManifestLayout,
        digests: [LibraryMonthKey: RemoteMonthManifestDigest],
        months: [RemoteLibraryMonthDelta]
    ) {
        guard let directory = cacheDirectory() else { return }
        do {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            let payload = Payload(
                version: Self.currentVersion,
                profileKey: profileKey,
                layout: Self.layoutIdentifier(layout),
                digests: digests.values.sorted { $0.month < $1.month }.map(DigestPayload.init),
                months: months.sorted { $0.month < $1.month }.map(MonthPayload.init)
            )
            let data = try JSONEncoder().encode(payload)
            try data.write(to: cacheURL(profileKey: profileKey), options: .atomic)
            try? FileProtection.enableBackgroundAccess(at: directory)
            try? FileProtection.enableBackgroundAccess(at: cacheURL(profileKey: profileKey))
        } catch {
            try? FileManager.default.removeItem(at: cacheURL(profileKey: profileKey))
        }
    }

    func invalidate(profileKey: String) {
        try? FileManager.default.removeItem(at: cacheURL(profileKey: profileKey))
    }

    private func cacheURL(profileKey: String) -> URL {
        (cacheDirectory() ?? FileManager.default.temporaryDirectory)
            .appendingPathComponent(Self.cacheFileName(profileKey: profileKey), isDirectory: false)
    }

    private func cacheDirectory() -> URL? {
        if let directory { return directory }
        return FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)
            .first?
            .appendingPathComponent("RemoteManifestSnapshotCache", isDirectory: true)
    }

    private static func cacheFileName(profileKey: String) -> String {
        let encoded = Data(profileKey.utf8).base64EncodedString()
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "=", with: "")
        return encoded + ".json"
    }

    private static func layoutIdentifier(_ layout: MonthManifestStore.ManifestLayout) -> String {
        switch layout {
        case .v1: return "v1"
        case .lite: return "lite"
        }
    }
}

private extension Data {
    init?(hexString: String) {
        guard hexString.count.isMultiple(of: 2) else { return nil }
        var bytes: [UInt8] = []
        bytes.reserveCapacity(hexString.count / 2)
        var index = hexString.startIndex
        while index < hexString.endIndex {
            let next = hexString.index(index, offsetBy: 2)
            guard let byte = UInt8(hexString[index ..< next], radix: 16) else { return nil }
            bytes.append(byte)
            index = next
        }
        self = Data(bytes)
    }
}
