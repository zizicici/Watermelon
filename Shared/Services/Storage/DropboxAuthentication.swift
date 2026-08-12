import Foundation

nonisolated struct DropboxAccessToken: Sendable, Equatable {
    let value: String
    let expiresAt: Date

    func isUsable(now: Date = Date(), leeway: TimeInterval = 60) -> Bool {
        !value.isEmpty && expiresAt.timeIntervalSince(now) > leeway
    }
}

protocol DropboxAccessTokenProviding: Sendable {
    var dropboxSharedState: DropboxSharedState? { get }

    func accessToken(
        for credential: DropboxCredentialBlob,
        appKey: String,
        forceRefresh: Bool
    ) async throws -> DropboxAccessToken
}

extension DropboxAccessTokenProviding {
    var dropboxSharedState: DropboxSharedState? { nil }
}

nonisolated enum DropboxAuthenticationError: LocalizedError, Sendable {
    case configurationMissing
    case reauthenticationRequired
    case accountMismatch
    case invalidResponse

    var errorDescription: String? {
        switch self {
        case .configurationMissing:
            return String(localized: "dropbox.error.auth.configurationMissing")
        case .reauthenticationRequired:
            return String(localized: "dropbox.error.auth.reauthenticationRequired")
        case .accountMismatch:
            return String(localized: "dropbox.error.auth.accountMismatch")
        case .invalidResponse:
            return String(localized: "dropbox.error.invalidResponse")
        }
    }
}
