import Foundation

struct BackupRunRequest: Sendable {
    let profile: ServerProfileRecord
    let password: String
    let onlyAssetLocalIdentifiers: Set<String>?
}
