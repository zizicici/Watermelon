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
