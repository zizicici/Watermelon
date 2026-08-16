import CryptoKit
import Foundation

nonisolated struct DropboxMetadata: Decodable, Sendable {
    let tag: String
    let name: String
    let clientModified: String?
    let serverModified: String?
    let contentHash: String?
    let size: Int64?

    private enum CodingKeys: String, CodingKey {
        case tag = ".tag"
        case name
        case clientModified = "client_modified"
        case serverModified = "server_modified"
        case contentHash = "content_hash"
        case size
    }
}

nonisolated enum DropboxContentHasher {
    private static let blockSize = 4 * 1024 * 1024

    static func hexDigest(of fileURL: URL) throws -> String {
        let handle = try FileHandle(forReadingFrom: fileURL)
        defer { try? handle.close() }
        var aggregate = SHA256()
        while true {
            try Task.checkCancellation()
            let block = try handle.read(upToCount: blockSize) ?? Data()
            guard !block.isEmpty else { break }
            aggregate.update(data: Data(SHA256.hash(data: block)))
        }
        return Data(aggregate.finalize()).map { String(format: "%02x", $0) }.joined()
    }
}

nonisolated struct DropboxListFolderPage: Decodable, Sendable {
    let entries: [DropboxMetadata]
    let cursor: String
    let hasMore: Bool

    private enum CodingKeys: String, CodingKey {
        case entries
        case cursor
        case hasMore = "has_more"
    }
}

nonisolated struct DropboxRelocationResult: Decodable, Sendable {
    let metadata: DropboxMetadata
}

nonisolated struct DropboxUploadSessionStartResult: Decodable, Sendable {
    let sessionID: String

    private enum CodingKeys: String, CodingKey {
        case sessionID = "session_id"
    }
}

nonisolated enum DropboxDateCodec {
    private static let fractionalFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    private static let formatter = ISO8601DateFormatter()

    static func date(from value: String?) -> Date? {
        guard let value else { return nil }
        return fractionalFormatter.date(from: value) ?? formatter.date(from: value)
    }

    static func string(from date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.string(from: date)
    }
}
