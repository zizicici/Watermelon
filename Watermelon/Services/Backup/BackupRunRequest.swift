import Foundation

struct BackupRunRequest: Sendable {
    let profile: ServerProfileRecord
    let password: String
    let onlyAssetLocalIdentifiers: Set<String>?
    let workerCountOverride: Int?

    init(
        profile: ServerProfileRecord,
        password: String,
        onlyAssetLocalIdentifiers: Set<String>?,
        workerCountOverride: Int? = nil
    ) {
        self.profile = profile
        self.password = password
        self.onlyAssetLocalIdentifiers = onlyAssetLocalIdentifiers
        self.workerCountOverride = workerCountOverride
    }
}
