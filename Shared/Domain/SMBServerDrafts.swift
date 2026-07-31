import CryptoKit
import Foundation

struct SMBServerLoginDraft: Sendable {
    var name: String
    var host: String
    var port: Int
    var username: String
    var domain: String?

    var effectivePort: Int { SMBEndpoint.effectivePort(port) }
}

struct SMBServerAuthContext: Sendable {
    var name: String
    var host: String
    var port: Int
    var username: String
    var password: String
    var domain: String?
}

struct SMBServerPathContext: Sendable {
    var auth: SMBServerAuthContext
    var shareName: String
    var basePath: String
}

struct SMBSelectionContextSignature: Equatable, Sendable {
    let host: String
    let port: Int
    let username: String
    let passwordDigest: Data
    let domain: String

    init(auth: SMBServerAuthContext) {
        host = RemoteHostIdentity.canonicalSMB(auth.host)
        port = SMBEndpoint.effectivePort(auth.port)
        username = auth.username.trimmingCharacters(in: .whitespacesAndNewlines)
        passwordDigest = Data(SHA256.hash(data: Data(auth.password.utf8)))
        domain = (auth.domain ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
    }
}

struct SMBSelectionContextBinding {
    private(set) var signature: SMBSelectionContextSignature?

    var isBound: Bool { signature != nil }

    mutating func bind(to signature: SMBSelectionContextSignature) {
        self.signature = signature
    }

    func matches(_ signature: SMBSelectionContextSignature) -> Bool {
        self.signature == signature
    }

    mutating func invalidateIfMismatched(
        _ current: SMBSelectionContextSignature?
    ) -> Bool {
        guard signature != nil, signature != current else { return false }
        signature = nil
        return true
    }

    mutating func clear() {
        signature = nil
    }
}
