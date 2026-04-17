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
                if let statusCode = ServerProfileRecord.webDAVErrorCode(from: error), statusCode == 401 {
                    return String(localized: "storage.error.webdav401")
                }
                if let statusCode = ServerProfileRecord.webDAVErrorCode(from: error), statusCode == 403 {
                    return String(localized: "storage.error.webdav403")
                }
            }
        }

        return error.localizedDescription
    }
}
