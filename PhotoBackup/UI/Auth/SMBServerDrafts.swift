import Foundation

struct SMBServerLoginDraft {
    var name: String
    var host: String
    var port: Int
    var username: String
    var domain: String?
}

struct SMBServerAuthContext {
    var name: String
    var host: String
    var port: Int
    var username: String
    var password: String
    var domain: String?
}

struct SMBServerPathContext {
    var auth: SMBServerAuthContext
    var shareName: String
    var basePath: String
}
