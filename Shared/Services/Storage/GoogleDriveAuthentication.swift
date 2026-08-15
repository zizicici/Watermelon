import Foundation

nonisolated enum GoogleDriveOAuthClientConfiguration {
    static func isValidClientID(_ value: String) -> Bool {
        let clientID = value.trimmingCharacters(in: .whitespacesAndNewlines)
        let suffix = ".apps.googleusercontent.com"
        guard clientID.hasSuffix(suffix) else { return false }
        let identifier = clientID.dropLast(suffix.count)
        guard !identifier.isEmpty else { return false }
        return identifier.allSatisfy { $0.isLetter || $0.isNumber || $0 == "-" }
    }

    static func callbackScheme(for clientID: String) throws -> String {
        let clientID = clientID.trimmingCharacters(in: .whitespacesAndNewlines)
        guard isValidClientID(clientID) else {
            throw GoogleDriveAuthenticationError.invalidClientID
        }
        return clientID.split(separator: ".").reversed().joined(separator: ".")
    }

    static func redirectURI(for clientID: String) throws -> String {
        try callbackScheme(for: clientID) + ":/oauth2redirect"
    }
}

nonisolated struct GoogleDriveAccessToken: Sendable, Equatable {
    let value: String
    let expiresAt: Date

    func isUsable(now: Date = Date(), leeway: TimeInterval = 60) -> Bool {
        !value.isEmpty && expiresAt.timeIntervalSince(now) > leeway
    }
}

protocol GoogleDriveAccessTokenProviding: Sendable {
    func accessToken(
        for credential: GoogleDriveCredentialBlob,
        clientID: String,
        forceRefresh: Bool
    ) async throws -> GoogleDriveAccessToken
}

nonisolated enum GoogleDriveAuthenticationError: LocalizedError, Sendable {
    case invalidClientID
    case reauthenticationRequired
    case accountMismatch
    case invalidResponse

    var errorDescription: String? {
        switch self {
        case .invalidClientID:
            return String(localized: "googledrive.error.auth.invalidClientID")
        case .reauthenticationRequired:
            return String(localized: "googledrive.error.auth.reauthenticationRequired")
        case .accountMismatch:
            return String(localized: "googledrive.error.auth.accountMismatch")
        case .invalidResponse:
            return String(localized: "googledrive.error.invalidResponse")
        }
    }
}
