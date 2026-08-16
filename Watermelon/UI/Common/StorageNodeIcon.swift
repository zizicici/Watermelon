import UIKit

enum StorageNodeIcon {
    static func image(
        for storageType: StorageType,
        pointSize: CGFloat = 20,
        configuration: UIImage.SymbolConfiguration? = nil
    ) -> UIImage? {
        if let assetName = assetName(for: storageType) {
            guard
                let image = UIImage(named: assetName),
                let cgImage = image.cgImage,
                image.size.height > 0
            else { return nil }
            let scale = image.scale * image.size.height / pointSize
            return UIImage(cgImage: cgImage, scale: scale, orientation: image.imageOrientation)
                .withRenderingMode(.alwaysTemplate)
        }
        return UIImage(systemName: storageType.symbolName, withConfiguration: configuration)
    }

    private static func assetName(for storageType: StorageType) -> String? {
        switch storageType {
        case .onedrive: "StorageOneDrive"
        case .dropbox: "StorageDropbox"
        case .googleDrive: "StorageGoogleDrive"
        case .smb, .webdav, .externalVolume, .s3, .sftp: nil
        }
    }
}
