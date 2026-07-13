import CryptoKit
import Foundation

struct BrowserLinkPairing: Equatable, Sendable {
    static let host = "link.watermelonbackup.com"

    let ticket: String
    let secret: Data
    let sessionID: String
    let issuedAt: Date
    let expiresAt: Date

    var signalingURL: URL {
        var components = URLComponents()
        components.scheme = "wss"
        components.host = Self.host
        components.path = "/ws/v1"
        components.queryItems = [
            URLQueryItem(name: "role", value: "phone"),
            URLQueryItem(name: "ticket", value: ticket),
        ]
        return components.url!
    }

    static func isCandidateURL(_ url: URL) -> Bool {
        url.scheme?.lowercased() == "https" &&
            url.host?.lowercased() == host &&
            isPairingPath(url.path)
    }

    static func parse(_ url: URL, now: Date = Date()) throws -> Self {
        guard isCandidateURL(url),
              url.port == nil,
              url.user == nil,
              url.password == nil,
              url.query == nil,
              isPairingPath(url.path),
              let fragment = url.fragment,
              let values = fragmentValues(fragment),
              let ticket = values["t"],
              let encodedSecret = values["s"],
              ticket.count == 98,
              encodedSecret.count == 43,
              let ticketBytes = Data(base64URLEncoded: ticket),
              let secret = Data(base64URLEncoded: encodedSecret),
              ticketBytes.count == 73,
              secret.count == 32,
              ticketBytes.base64URLEncodedString() == ticket,
              secret.base64URLEncodedString() == encodedSecret,
              ticketBytes[0] == 1 else {
            throw BrowserLinkPairingError.invalidLink
        }

        let session = ticketBytes.subdata(in: 1..<17)
        let commitment = ticketBytes.subdata(in: 17..<49)
        var commitmentInput = Data("watermelon-link-capability-v1:".utf8)
        commitmentInput.append(secret)
        let expectedCommitment = Data(SHA256.hash(data: commitmentInput))
        guard commitment == expectedCommitment else {
            throw BrowserLinkPairingError.invalidLink
        }

        let issued = ticketBytes.uint32BigEndian(at: 49)
        let expires = ticketBytes.uint32BigEndian(at: 53)
        let current = UInt64(max(0, floor(now.timeIntervalSince1970)))
        guard expires > issued,
              expires - issued <= 90,
              UInt64(expires) > current,
              UInt64(issued) <= current + 30 else {
            throw expires <= current ? BrowserLinkPairingError.expired : BrowserLinkPairingError.invalidLink
        }

        return Self(
            ticket: ticket,
            secret: secret,
            sessionID: session.base64URLEncodedString(),
            issuedAt: Date(timeIntervalSince1970: TimeInterval(issued)),
            expiresAt: Date(timeIntervalSince1970: TimeInterval(expires))
        )
    }

    private static func isPairingPath(_ path: String) -> Bool {
        let components = path.split(separator: "/", omittingEmptySubsequences: true)
        if components.count == 1 { return components[0] == "pair" }
        guard components.count == 2, components[1] == "pair" else { return false }
        return ["de", "es", "es-419", "fr", "ja", "ko", "pt-BR", "pt-PT", "ru", "uk", "zh-Hans", "zh-Hant"]
            .contains(String(components[0]))
    }

    private static func fragmentValues(_ fragment: String) -> [String: String]? {
        guard let items = URLComponents(string: "?\(fragment)")?.queryItems else { return nil }
        var values: [String: String] = [:]
        for item in items {
            guard let value = item.value, values[item.name] == nil else { return nil }
            values[item.name] = value
        }
        return values.count == 2 ? values : nil
    }
}

enum BrowserLinkPairingError: LocalizedError, Equatable {
    case invalidLink
    case expired

    var errorDescription: String? {
        switch self {
        case .invalidLink: String(localized: "link.error.invalid")
        case .expired: String(localized: "link.error.expired")
        }
    }
}

extension Data {
    init?(base64URLEncoded value: String) {
        guard value.range(of: "^[A-Za-z0-9_-]*$", options: .regularExpression) != nil else { return nil }
        let base64 = value
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
            .padding(toMultipleOf: 4, with: "=")
        self.init(base64Encoded: base64)
    }

    func base64URLEncodedString() -> String {
        base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    fileprivate func uint32BigEndian(at offset: Int) -> UInt32 {
        self[offset..<(offset + 4)].reduce(UInt32.zero) { ($0 << 8) | UInt32($1) }
    }
}

private extension String {
    func padding(toMultipleOf divisor: Int, with character: Character) -> String {
        let remainder = count % divisor
        guard remainder != 0 else { return self }
        return self + String(repeating: character, count: divisor - remainder)
    }
}
