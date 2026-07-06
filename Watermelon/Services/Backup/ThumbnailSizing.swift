import UIKit

enum ThumbnailSizing {
    static let maximumLongSide = 400
    static let jpegCompressionQuality: CGFloat = 0.6

    static func targetLongSide(originalWidth: Int, originalHeight: Int, cap: Int = maximumLongSide) -> Int? {
        guard originalWidth > 0, originalHeight > 0, cap > 0 else { return nil }
        return max(1, min(max(originalWidth, originalHeight) / 2, cap))
    }

    static func fittedSize(width: Int, height: Int, maximumLongSide: Int) -> CGSize? {
        guard width > 0, height > 0, maximumLongSide > 0 else { return nil }
        let longSide = min(max(width, height), maximumLongSide)
        let scale = CGFloat(longSide) / CGFloat(max(width, height))
        let targetWidth = max(1, Int((CGFloat(width) * scale).rounded(.down)))
        let targetHeight = max(1, Int((CGFloat(height) * scale).rounded(.down)))
        return CGSize(width: targetWidth, height: targetHeight)
    }

    static func fittedImage(_ image: UIImage, maximumLongSide: Int) -> UIImage? {
        let width = Int((image.size.width * image.scale).rounded(.down))
        let height = Int((image.size.height * image.scale).rounded(.down))
        guard let targetSize = fittedSize(width: width, height: height, maximumLongSide: maximumLongSide) else {
            return nil
        }
        let format = UIGraphicsImageRendererFormat()
        format.scale = 1
        format.opaque = true
        return UIGraphicsImageRenderer(size: targetSize, format: format).image { _ in
            image.draw(in: CGRect(origin: .zero, size: targetSize))
        }
    }
}
