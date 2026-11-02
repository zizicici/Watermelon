import Foundation

enum UserFacingErrorLocalizer {
    static func message(
        for error: Error,
        profile: ServerProfileRecord? = nil,
        storageType: StorageType? = nil
    ) -> String {
        let error = StorageErrorMessages.unwrapped(error)
        if error is LiteRepoError || profile?.isBrowserLinkProfile == true { return error.localizedDescription }
        if let storageType = profile?.resolvedStorageType ?? storageType {
            switch storageType {
            case .externalVolume:
                if RemoteStorageClientError.isLikelyExternalStorageUnavailable(error) {
                    return String(localized: "storage.error.externalUnavailable")
                }
            case .smb:
                return SMBErrorClassifier.describe(error)
            case .webdav:
                return WebDAVErrorClassifier.describe(error)
            case .s3:
                return S3ErrorClassifier.describe(error)
            case .sftp:
                return SFTPErrorClassifier.describe(error)
            case .onedrive:
                return OneDriveErrorClassifier.describe(error)
            case .dropbox:
                return DropboxErrorClassifier.describe(error)
            case .googleDrive:
                return GoogleDriveErrorClassifier.describe(error)
            }
        }

        return StorageErrorMessages.fallback(error)
    }
}
