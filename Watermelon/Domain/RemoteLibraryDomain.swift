import Foundation

struct RemoteManifestResource: Hashable, Identifiable {
    let year: Int
    let month: Int
    let fileName: String
    let contentHash: Data
    let fileSize: Int64
    let resourceType: Int
    let creationDateNs: Int64?
    let backedUpAtNs: Int64

    var id: String {
        monthKey + "/" + fileName
    }

    var monthKey: String {
        String(format: "%04d-%02d", year, month)
    }

    var remoteRelativePath: String {
        String(format: "%04d/%02d/%@", year, month, fileName)
    }

    var creationDate: Date {
        if let creationDateNs {
            return Date(timeIntervalSince1970: Double(creationDateNs) / 1_000_000_000)
        }
        return Date(timeIntervalSince1970: Double(backedUpAtNs) / 1_000_000_000)
    }

    var contentHashHex: String {
        contentHash.map { String(format: "%02x", $0) }.joined()
    }
}

struct RemoteLibrarySnapshot {
    let resources: [RemoteManifestResource]

    var totalCount: Int {
        resources.count
    }

    var hashSet: Set<Data> {
        Set(resources.map(\.contentHash))
    }

    var countsByMonth: [String: Int] {
        Dictionary(grouping: resources, by: \.monthKey).mapValues(\.count)
    }
}

enum ResourceTypeCode {
    static let unknown = 0
    static let photo = 1
    static let video = 2
    static let audio = 3
    static let alternatePhoto = 4
    static let fullSizePhoto = 5
    static let fullSizeVideo = 6
    static let pairedVideo = 7
    static let adjustmentData = 8
    static let adjustmentBasePhoto = 9
    static let photoProxy = 10

    static func isPhotoLike(_ code: Int) -> Bool {
        code == photo || code == alternatePhoto || code == fullSizePhoto || code == adjustmentBasePhoto || code == photoProxy
    }

    static func isVideoLike(_ code: Int) -> Bool {
        code == video || code == fullSizeVideo || code == pairedVideo
    }
}
