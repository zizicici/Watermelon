import Foundation

nonisolated struct OneDriveAccessToken: Sendable {
    let value: String
    let expiresAt: Date?

    func isUsable(now: Date = Date()) -> Bool {
        guard !value.isEmpty else { return false }
        guard let expiresAt else { return true }
        return expiresAt.timeIntervalSince(now) > 60
    }
}

nonisolated protocol OneDriveAccessTokenProviding: Sendable {
    func accessToken(
        for credential: OneDriveCredentialBlob,
        forceRefresh: Bool,
        claims: String?
    ) async throws -> OneDriveAccessToken
}

nonisolated enum OneDriveAuthenticationError: LocalizedError, Sendable {
    case configurationMissing
    case reauthenticationRequired
    case accountMismatch
    case unsupportedAccount

    var errorDescription: String? {
        switch self {
        case .configurationMissing:
            return String(localized: "onedrive.error.auth.configurationMissing")
        case .reauthenticationRequired:
            return String(localized: "onedrive.error.auth.reauthenticationRequired")
        case .accountMismatch:
            return String(localized: "onedrive.error.auth.accountMismatch")
        case .unsupportedAccount:
            return String(localized: "onedrive.error.auth.unsupportedAccount")
        }
    }
}
