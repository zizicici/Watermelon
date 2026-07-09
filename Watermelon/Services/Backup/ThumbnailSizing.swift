import ImageIO
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
        return opaqueImage(size: targetSize) {
            image.draw(in: CGRect(origin: .zero, size: targetSize))
        }
    }

    static func jpegData(from image: UIImage, compressionQuality: CGFloat = jpegCompressionQuality) -> Data? {
        guard let cgImage = opaqueCGImage(from: image) else { return nil }
        let data = NSMutableData()
        guard let destination = CGImageDestinationCreateWithData(
            data,
            "public.jpeg" as CFString,
            1,
            nil
        ) else { return nil }
        let options = [
            kCGImageDestinationLossyCompressionQuality: compressionQuality
        ] as CFDictionary
        CGImageDestinationAddImage(destination, cgImage, options)
        guard CGImageDestinationFinalize(destination) else { return nil }
        return data as Data
    }

    private static func opaqueImage(size: CGSize, drawing: () -> Void) -> UIImage? {
        guard let cgImage = opaqueCGImage(size: size, drawing: drawing) else { return nil }
        return UIImage(cgImage: cgImage, scale: 1, orientation: .up)
    }

    private static func opaqueCGImage(from image: UIImage) -> CGImage? {
        let width = Int((image.size.width * image.scale).rounded(.down))
        let height = Int((image.size.height * image.scale).rounded(.down))
        guard width > 0, height > 0 else { return nil }
        let size = CGSize(width: width, height: height)
        return opaqueCGImage(size: size) {
            image.draw(in: CGRect(origin: .zero, size: size))
        }
    }

    private static func opaqueCGImage(size: CGSize, drawing: () -> Void) -> CGImage? {
        let width = Int(size.width.rounded(.down))
        let height = Int(size.height.rounded(.down))
        guard width > 0, height > 0 else { return nil }
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        let bitmapInfo = CGBitmapInfo.byteOrder32Big.rawValue | CGImageAlphaInfo.noneSkipLast.rawValue
        guard let context = CGContext(
            data: nil,
            width: width,
            height: height,
            bitsPerComponent: 8,
            bytesPerRow: 0,
            space: colorSpace,
            bitmapInfo: bitmapInfo
        ) else { return nil }
        context.interpolationQuality = .high
        context.setFillColor(UIColor.white.cgColor)
        context.fill(CGRect(x: 0, y: 0, width: width, height: height))
        context.translateBy(x: 0, y: CGFloat(height))
        context.scaleBy(x: 1, y: -1)
        UIGraphicsPushContext(context)
        drawing()
        UIGraphicsPopContext()
        return context.makeImage()
    }
}
