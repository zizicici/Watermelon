import Foundation
#if os(iOS)
import Photos
#endif

struct RemoteManifestAsset: Hashable, Identifiable {
    let year: Int
    let month: Int
    let assetFingerprint: Data
    let creationDateMs: Int64?
    let backedUpAtMs: Int64
    let resourceCount: Int
    let totalFileSizeBytes: Int64

    var id: String {
        monthKey + "/" + assetFingerprintHex
    }

    var monthKey: String {
        String(format: "%04d-%02d", year, month)
    }

    var creationDate: Date {
        if let creationDateMs {
            return Date(millisecondsSinceEpoch: creationDateMs)
        }
        return Date(millisecondsSinceEpoch: backedUpAtMs)
    }

    var assetFingerprintHex: String {
        assetFingerprint.hexString
    }
}

struct RemoteAssetResourceLink: Hashable {
    let year: Int
    let month: Int
    let assetFingerprint: Data
    let resourceHash: Data
    let resourceFileName: String?
    let role: Int
    let slot: Int

    init(
        year: Int,
        month: Int,
        assetFingerprint: Data,
        resourceHash: Data,
        resourceFileName: String? = nil,
        role: Int,
        slot: Int
    ) {
        self.year = year
        self.month = month
        self.assetFingerprint = assetFingerprint
        self.resourceHash = resourceHash
        self.resourceFileName = resourceFileName
        self.role = role
        self.slot = slot
    }

    var monthKey: String {
        String(format: "%04d-%02d", year, month)
    }

    var assetID: String {
        monthKey + "/" + assetFingerprint.hexString
    }
}

struct RemoteResourceLookup {
    private let resourceByFileName: [String: RemoteManifestResource]
    private let uniqueResourceByHash: [Data: RemoteManifestResource]

    init(_ resources: [RemoteManifestResource]) {
        var byFileName: [String: RemoteManifestResource] = [:]
        byFileName.reserveCapacity(resources.count)
        var byHash: [Data: RemoteManifestResource] = [:]
        var ambiguousHashes = Set<Data>()
        for resource in resources {
            byFileName[resource.fileName] = resource
            if let existing = byHash[resource.contentHash], existing.fileName != resource.fileName {
                ambiguousHashes.insert(resource.contentHash)
            } else {
                byHash[resource.contentHash] = resource
            }
        }
        for hash in ambiguousHashes {
            byHash.removeValue(forKey: hash)
        }
        self.resourceByFileName = byFileName
        self.uniqueResourceByHash = byHash
    }

    func resource(for link: RemoteAssetResourceLink) -> RemoteManifestResource? {
        if let fileName = link.resourceFileName {
            guard let resource = resourceByFileName[fileName],
                  resource.contentHash == link.resourceHash else {
                return nil
            }
            return resource
        }
        return uniqueResourceByHash[link.resourceHash]
    }

    func contains(_ link: RemoteAssetResourceLink) -> Bool {
        resource(for: link) != nil
    }
}

struct RemoteManifestResource: Hashable, Identifiable {
    static let plaintextStorageCodec = 0
    static let encryptedStorageCodec = 1
    static let contentHashByteCount = 32

    let year: Int
    let month: Int
    let fileName: String
    let contentHash: Data
    let fileSize: Int64
    let resourceType: Int
    let creationDateMs: Int64?
    let backedUpAtMs: Int64
    let storageCodec: Int
    let storedFileSize: Int64?
    let encryptionKeyID: String?

    init(
        year: Int,
        month: Int,
        fileName: String,
        contentHash: Data,
        fileSize: Int64,
        resourceType: Int,
        creationDateMs: Int64?,
        backedUpAtMs: Int64,
        storageCodec: Int = plaintextStorageCodec,
        storedFileSize: Int64? = nil,
        encryptionKeyID: String? = nil
    ) {
        self.year = year
        self.month = month
        self.fileName = fileName
        self.contentHash = contentHash
        self.fileSize = fileSize
        self.resourceType = resourceType
        self.creationDateMs = creationDateMs
        self.backedUpAtMs = backedUpAtMs
        self.storageCodec = storageCodec
        self.storedFileSize = storedFileSize
        self.encryptionKeyID = encryptionKeyID
    }

    var id: String {
        monthKey + "/" + fileName
    }

    var monthKey: String {
        String(format: "%04d-%02d", year, month)
    }

    var remoteRelativePath: String {
        String(format: "%04d/%02d/%@", year, month, fileName)
    }

    var contentHashHex: String {
        contentHash.hexString
    }

    var isEncrypted: Bool {
        storageCodec == Self.encryptedStorageCodec
    }
}

struct RemoteAssetResourceInstance: Hashable, Identifiable, Sendable {
    static let plaintextStorageCodec = RemoteManifestResource.plaintextStorageCodec
    static let encryptedStorageCodec = RemoteManifestResource.encryptedStorageCodec

    let role: Int
    let slot: Int
    let resourceHash: Data
    let fileName: String
    let fileSize: Int64
    let remoteRelativePath: String
    let creationDateMs: Int64?
    let storageCodec: Int
    let storedFileSize: Int64?
    let encryptionKeyID: String?

    init(
        role: Int,
        slot: Int,
        resourceHash: Data,
        fileName: String,
        fileSize: Int64,
        remoteRelativePath: String,
        creationDateMs: Int64?,
        storageCodec: Int = plaintextStorageCodec,
        storedFileSize: Int64? = nil,
        encryptionKeyID: String? = nil
    ) {
        self.role = role
        self.slot = slot
        self.resourceHash = resourceHash
        self.fileName = fileName
        self.fileSize = fileSize
        self.remoteRelativePath = remoteRelativePath
        self.creationDateMs = creationDateMs
        self.storageCodec = storageCodec
        self.storedFileSize = storedFileSize
        self.encryptionKeyID = encryptionKeyID
    }

    var id: String {
        "\(role)|\(slot)|\(resourceHash.hexString)"
    }

    #if os(iOS)
    var resourceType: PHAssetResourceType? {
        guard role > 0 else { return nil }
        return PHAssetResourceType(rawValue: role)
    }
    #endif

    var contentHashHex: String {
        resourceHash.hexString
    }

    var isEncrypted: Bool {
        storageCodec == Self.encryptedStorageCodec
    }
}

/// Cheap summary of a remote manifest sync. Does not materialize the per-asset
/// resource/link arrays, so it's safe to hand to callers that only need totals (log lines,
/// gating decisions). When a caller actually needs the flat arrays, ask the service for a
/// full `RemoteLibrarySnapshot` explicitly.
struct RemoteIndexSyncDigest: Sendable {
    let resourceCount: Int
    let assetCount: Int
    let linkCount: Int

    var totalEntryCount: Int { resourceCount + assetCount + linkCount }
}

struct RemoteLibrarySnapshot {
    let resources: [RemoteManifestResource]
    let assets: [RemoteManifestAsset]
    let assetResourceLinks: [RemoteAssetResourceLink]

    init(
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        assetResourceLinks: [RemoteAssetResourceLink] = []
    ) {
        self.resources = resources
        self.assets = assets
        self.assetResourceLinks = assetResourceLinks
    }

    var totalCount: Int {
        assets.count
    }

    var totalResourceCount: Int {
        resources.count
    }
}

extension Notification.Name {
    static let MonthGroupingTimeZonePreferenceDidChange = Notification.Name("com.zizicici.watermelon.monthGroupingTimeZonePreferenceDidChange")
}

struct MonthGroupingTimeZonePreference: Codable, Hashable, Sendable {
    enum Mode: String, Codable, Sendable {
        case system
        case fixedIana
        case fixedOffset
    }

    static let storageKey = "com.zizicici.common.settings.MonthGroupingTimeZonePreference"
    static let currentVersion = 1
    static let defaultPreference = MonthGroupingTimeZonePreference(mode: .system)

    var version: Int = currentVersion
    var mode: Mode
    var identifier: String?
    var fallbackOffsetSeconds: Int?
    var offsetSeconds: Int?

    private enum CodingKeys: String, CodingKey {
        case version
        case mode
        case identifier
        case fallbackOffsetSeconds
        case offsetSeconds
    }

    init(
        version: Int = Self.currentVersion,
        mode: Mode,
        identifier: String? = nil,
        fallbackOffsetSeconds: Int? = nil,
        offsetSeconds: Int? = nil
    ) {
        self.version = version
        self.mode = mode
        self.identifier = identifier
        self.fallbackOffsetSeconds = fallbackOffsetSeconds
        self.offsetSeconds = offsetSeconds
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        version = try container.decodeIfPresent(Int.self, forKey: .version) ?? Self.currentVersion
        mode = try container.decode(Mode.self, forKey: .mode)
        identifier = try container.decodeIfPresent(String.self, forKey: .identifier)
        fallbackOffsetSeconds = try container.decodeIfPresent(Int.self, forKey: .fallbackOffsetSeconds)
        offsetSeconds = try container.decodeIfPresent(Int.self, forKey: .offsetSeconds)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(version, forKey: .version)
        try container.encode(mode, forKey: .mode)
        try container.encodeIfPresent(identifier, forKey: .identifier)
        try container.encodeIfPresent(fallbackOffsetSeconds, forKey: .fallbackOffsetSeconds)
        try container.encodeIfPresent(offsetSeconds, forKey: .offsetSeconds)
    }

    static func == (lhs: MonthGroupingTimeZonePreference, rhs: MonthGroupingTimeZonePreference) -> Bool {
        guard lhs.mode == rhs.mode else { return false }
        switch lhs.mode {
        case .system:
            return true
        case .fixedIana:
            guard lhs.identifier == rhs.identifier else { return false }
            if let identifier = lhs.identifier, TimeZone(identifier: identifier) != nil {
                return true
            }
            return lhs.fallbackOffsetSeconds == rhs.fallbackOffsetSeconds
        case .fixedOffset:
            return lhs.offsetSeconds == rhs.offsetSeconds
        }
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(mode)
        switch mode {
        case .system:
            break
        case .fixedIana:
            hasher.combine(identifier)
            if identifier.flatMap(TimeZone.init(identifier:)) == nil {
                hasher.combine(fallbackOffsetSeconds)
            }
        case .fixedOffset:
            hasher.combine(offsetSeconds)
        }
    }

    static var current: MonthGroupingTimeZonePreference {
        guard let raw = UserDefaults.standard.string(forKey: storageKey),
              let data = raw.data(using: .utf8),
              let decoded = try? JSONDecoder().decode(MonthGroupingTimeZonePreference.self, from: data),
              decoded.version == currentVersion else {
            return defaultPreference
        }
        return decoded.normalized()
    }

    static func setCurrent(_ value: MonthGroupingTimeZonePreference) {
        let normalized = value.normalized()
        guard normalized != current else { return }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        guard let data = try? encoder.encode(normalized),
              let raw = String(data: data, encoding: .utf8) else {
            return
        }
        UserDefaults.standard.set(raw, forKey: storageKey)
        NotificationCenter.default.post(name: .MonthGroupingTimeZonePreferenceDidChange, object: nil)
    }

    static func currentSystemTimeZone() -> TimeZone {
        NSTimeZone.resetSystemTimeZone()
        return TimeZone.current
    }

    static func fixedCurrent() -> MonthGroupingTimeZonePreference {
        let timeZone = currentSystemTimeZone()
        return MonthGroupingTimeZonePreference(
            mode: .fixedIana,
            identifier: timeZone.identifier,
            fallbackOffsetSeconds: timeZone.secondsFromGMT(for: Date())
        )
    }

    static func fixedUTC() -> MonthGroupingTimeZonePreference {
        MonthGroupingTimeZonePreference(mode: .fixedOffset, offsetSeconds: 0)
    }

    static func frozenCurrent(at date: Date = Date()) -> MonthGroupingTimeZonePreference {
        current.frozen(at: date)
    }

    var effectiveTimeZone: TimeZone {
        switch mode {
        case .system:
            return .current
        case .fixedIana:
            if let identifier, let timeZone = TimeZone(identifier: identifier) {
                return timeZone
            }
            if let fallbackOffsetSeconds, let timeZone = TimeZone(secondsFromGMT: fallbackOffsetSeconds) {
                return timeZone
            }
            return .current
        case .fixedOffset:
            if let offsetSeconds, let timeZone = TimeZone(secondsFromGMT: offsetSeconds) {
                return timeZone
            }
            return .current
        }
    }

    func normalized() -> MonthGroupingTimeZonePreference {
        switch mode {
        case .system:
            return MonthGroupingTimeZonePreference(version: Self.currentVersion, mode: .system)
        case .fixedIana:
            guard let identifier, !identifier.isEmpty else {
                return .defaultPreference
            }
            guard TimeZone(identifier: identifier) != nil
                    || fallbackOffsetSeconds.flatMap(TimeZone.init(secondsFromGMT:)) != nil else {
                return .defaultPreference
            }
            return MonthGroupingTimeZonePreference(
                version: Self.currentVersion,
                mode: .fixedIana,
                identifier: identifier,
                fallbackOffsetSeconds: fallbackOffsetSeconds
            )
        case .fixedOffset:
            guard let offsetSeconds, TimeZone(secondsFromGMT: offsetSeconds) != nil else {
                return .defaultPreference
            }
            return MonthGroupingTimeZonePreference(
                version: Self.currentVersion,
                mode: .fixedOffset,
                offsetSeconds: offsetSeconds
            )
        }
    }

    func frozen(at date: Date = Date()) -> MonthGroupingTimeZonePreference {
        switch mode {
        case .system:
            let timeZone = Self.currentSystemTimeZone()
            return MonthGroupingTimeZonePreference(
                version: Self.currentVersion,
                mode: .fixedIana,
                identifier: timeZone.identifier,
                fallbackOffsetSeconds: timeZone.secondsFromGMT(for: date)
            )
        case .fixedIana, .fixedOffset:
            return normalized()
        }
    }
}

struct LibraryMonthKey: Hashable, Comparable, Sendable {
    let year: Int
    let month: Int

    // nonisolated for off-main-actor filename use (RepoLayoutLite); displayText's shared DateFormatter stays actor-isolated.
    nonisolated var text: String {
        String(format: "%04d-%02d", year, month)
    }

    var displayText: String {
        let components = DateComponents(year: year, month: month)
        guard let date = Calendar.current.date(from: components) else {
            return text
        }
        return Self.displayTextFormatter.string(from: date)
    }

    private static let displayTextFormatter: DateFormatter = {
        let f = DateFormatter()
        f.setLocalizedDateFormatFromTemplate("yyyyMMM")
        return f
    }()

    static func < (lhs: LibraryMonthKey, rhs: LibraryMonthKey) -> Bool {
        if lhs.year == rhs.year {
            return lhs.month < rhs.month
        }
        return lhs.year < rhs.year
    }

    static func currentPreferenceMonthCalendar() -> Calendar {
        monthCalendar(preference: .current)
    }

    static func monthCalendar(preference: MonthGroupingTimeZonePreference) -> Calendar {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = preference.effectiveTimeZone
        return calendar
    }

    static func from(date: Date?) -> LibraryMonthKey {
        from(date: date, calendar: currentPreferenceMonthCalendar())
    }

    static func from(date: Date?, calendar: Calendar) -> LibraryMonthKey {
        let date = date ?? Date(timeIntervalSince1970: 0)
        let comps = calendar.dateComponents([.year, .month], from: date)
        return LibraryMonthKey(year: comps.year ?? 1970, month: comps.month ?? 1)
    }
}

struct RemoteLibraryMonthDelta {
    let month: LibraryMonthKey
    let resources: [RemoteManifestResource]
    let assets: [RemoteManifestAsset]
    let assetResourceLinks: [RemoteAssetResourceLink]
}

struct RemoteMonthManifestDigest: Hashable {
    let month: LibraryMonthKey
    let manifestSize: Int64
    let manifestModifiedAtMs: Int64?
}

// Per-month remote data resolved for display: only assets with a resolvable link are counted, sizes are
// deduped over reachable resource hashes (the partial-flush drop rule). Neutral intermediate produced by
// RemoteMonthResolver and mapped onto Home types by HomeRemoteIndexEngine.apply.
struct RemoteMonthResolved: Hashable, Sendable {
    let month: LibraryMonthKey
    let assetCount: Int
    let photoCount: Int
    let videoCount: Int
    let totalSizeBytes: Int64
    let fingerprints: Set<Data>
}

struct RemoteLibrarySnapshotState {
    let revision: UInt64
    let isFullSnapshot: Bool
    let monthDeltas: [RemoteLibraryMonthDelta]
    // Identity of the profile this snapshot belongs to (nil = no remote context established yet). Lets a
    // reader reject a snapshot that was reset to a different profile than the one it was built for.
    let profileKey: String?
}

enum RepoUpgradePhase: Hashable, Sendable {
    case copying      // per-month manifest relocation
    case validating   // per-month byte re-verification
    case finalizing   // prune-marker write + version.json commit (indeterminate)
    case cleaning     // per-month legacy-V1 prune + orphan cleanup
}

struct RemoteSyncProgress: Hashable, Sendable {
    enum Kind: Hashable, Sendable {
        case scanningRemoteIndex
        case remoteIndex
        case repoUpgrade(RepoUpgradePhase)
    }

    let current: Int
    let total: Int
    let kind: Kind

    init(current: Int, total: Int, kind: Kind = .remoteIndex) {
        self.current = current
        self.total = total
        self.kind = kind
    }
}

enum ResourceTypeCode {
    static let photo = 1              // PHAssetResourceType.photo
    static let video = 2              // .video
    static let audio = 3              // .audio
    static let alternatePhoto = 4     // .alternatePhoto
    static let fullSizePhoto = 5      // .fullSizePhoto
    static let fullSizeVideo = 6      // .fullSizeVideo
    static let adjustmentData = 7     // .adjustmentData
    static let adjustmentBasePhoto = 8 // .adjustmentBasePhoto
    static let pairedVideo = 9        // .pairedVideo
    static let fullSizePairedVideo = 10 // .fullSizePairedVideo
    static let adjustmentBasePairedVideo = 11 // .adjustmentBasePairedVideo
    static let adjustmentBaseVideo = 12 // .adjustmentBaseVideo
    static let photoProxy = 19        // .photoProxy

    // Thin forwarders to the single role model (see ResourceRole) — kept so existing call sites don't churn.
    static func isPhotoLike(_ code: Int) -> Bool { ResourceRole.isPhotoSide(code) }
    static func isPairedVideo(_ code: Int) -> Bool { ResourceRole.isPairedVideoSide(code) }
    static func isVideoLike(_ code: Int) -> Bool { ResourceRole.isVideoSide(code) }
}
