import Foundation

enum UserFacingErrorLocalizer {
    static func message(
        for error: Error,
        profile: ServerProfileRecord? = nil,
        storageType: StorageType? = nil
    ) -> String {
        if let profile {
            return profile.userFacingStorageErrorMessage(error)
        }

        if let storageType {
            switch storageType {
            case .externalVolume:
                if RemoteStorageClientError.isLikelyExternalStorageUnavailable(error) {
                    return String(localized: "storage.error.externalUnavailable")
                }
            case .smb:
                if SMBErrorClassifier.isConnectionUnavailable(error) {
                    return String(localized: "storage.error.smbUnavailable")
                }
            case .webdav:
                return WebDAVErrorClassifier.describe(error)
            }
        }

        return error.localizedDescription
    }
}
