import Foundation

nonisolated struct OneDriveDriveItem: Decodable, Sendable {
    struct Facet: Decodable, Sendable {}

    struct FileFacet: Decodable, Sendable {}

    struct ParentReference: Decodable, Sendable {
        let driveId: String?
        let id: String?
    }

    struct FileSystemInfo: Decodable, Sendable {
        let createdDateTime: String?
        let lastModifiedDateTime: String?
    }

    let id: String
    let name: String
    let size: Int64?
    let eTag: String?
    let cTag: String?
    let createdDateTime: String?
    let lastModifiedDateTime: String?
    let folder: Facet?
    let file: FileFacet?
    let parentReference: ParentReference?
    let fileSystemInfo: FileSystemInfo?
}

nonisolated struct OneDriveDriveItemPage: Decodable, Sendable {
    let value: [OneDriveDriveItem]
    let nextLink: String?

    private enum CodingKeys: String, CodingKey {
        case value
        case nextLink = "@odata.nextLink"
    }
}

nonisolated struct OneDriveUploadSession: Decodable, Sendable {
    let uploadUrl: String
    let nextExpectedRanges: [String]?
}

nonisolated struct OneDriveUploadStatus: Decodable, Sendable {
    let nextExpectedRanges: [String]?
}

nonisolated struct OneDriveAsyncOperationStatus: Decodable, Sendable {
    struct OperationError: Decodable, Sendable {
        let code: String?
    }

    let status: String?
    let resourceId: String?
    let error: OperationError?
}

nonisolated enum OneDriveJSON {
    static func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        do {
            return try JSONDecoder().decode(type, from: data)
        } catch {
            throw OneDriveErrorClassifier.makeServiceError(
                statusCode: -1,
                code: "invalidResponse",
                message: String(localized: "onedrive.error.graph.invalidResponse"),
                retryAfter: nil,
                claims: nil
            )
        }
    }

    static func body(_ object: [String: Any]) throws -> Data {
        guard JSONSerialization.isValidJSONObject(object) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return try JSONSerialization.data(withJSONObject: object)
    }

    static func errorCode(from data: Data) -> String? {
        guard let root = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let error = root["error"] as? [String: Any] else { return nil }
        return error["code"] as? String
    }

    static func errorMessage(from data: Data) -> String? {
        guard let root = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let error = root["error"] as? [String: Any],
              let message = error["message"] as? String else { return nil }
        let trimmed = message.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}

nonisolated enum OneDriveDateCodec {
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
        fractionalFormatter.string(from: date)
    }
}
