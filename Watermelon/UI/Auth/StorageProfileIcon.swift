import UIKit

enum StorageProfileIcon {
    private static let containerSize = CGSize(width: 30, height: 30)
    private static let cornerRadius: CGFloat = 7
    private static let symbolPointSize: CGFloat = 16

    private static var cache: [StorageType: UIImage] = [:]

    static func image(for storageType: StorageType) -> UIImage {
        if let cached = cache[storageType] { return cached }
        let rendered = render(for: storageType)
        cache[storageType] = rendered
        return rendered
    }

    private static func render(for storageType: StorageType) -> UIImage {
        let renderer = UIGraphicsImageRenderer(size: containerSize)
        return renderer.image { _ in
            let rect = CGRect(origin: .zero, size: containerSize)
            UIBezierPath(roundedRect: rect, cornerRadius: cornerRadius).addClip()
            backgroundColor(for: storageType).setFill()
            UIRectFill(rect)

            let symbolConfig = UIImage.SymbolConfiguration(pointSize: symbolPointSize, weight: .semibold)
            guard let symbol = UIImage(systemName: symbolName(for: storageType), withConfiguration: symbolConfig)?
                .withTintColor(.white, renderingMode: .alwaysOriginal)
            else { return }

            let imageSize = symbol.size
            let drawRect = CGRect(
                x: (containerSize.width - imageSize.width) / 2,
                y: (containerSize.height - imageSize.height) / 2,
                width: imageSize.width,
                height: imageSize.height
            )
            symbol.draw(in: drawRect)
        }
    }

    private static func backgroundColor(for storageType: StorageType) -> UIColor {
        switch storageType {
        case .smb:
            return .Material.Blue._500
        case .webdav:
            return .Material.Teal._500
        case .externalVolume:
            return .Material.Orange._500
        }
    }

    private static func symbolName(for storageType: StorageType) -> String {
        switch storageType {
        case .smb:
            return "server.rack"
        case .webdav:
            return "network"
        case .externalVolume:
            return "externaldrive"
        }
    }
}
