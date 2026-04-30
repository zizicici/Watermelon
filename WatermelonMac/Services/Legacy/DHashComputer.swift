import CoreGraphics
import Foundation
import ImageIO

enum DHashError: Error {
    case cannotOpenSource
    case decodeFailed
    case contextCreationFailed
}

/// Computes a 64-bit difference hash (dHash) for an image at a local URL.
/// Same shot in different containers (HEIC vs JPEG) or compression levels yields the same
/// or near-identical dHash; cropping or strong filters change it.
enum DHashComputer {
    private static let width = 9
    private static let height = 8

    static func compute(url: URL) throws -> Data {
        guard let source = CGImageSourceCreateWithURL(url as CFURL, nil) else {
            throw DHashError.cannotOpenSource
        }
        let options: [CFString: Any] = [
            kCGImageSourceCreateThumbnailFromImageIfAbsent: true,
            kCGImageSourceShouldCacheImmediately: true,
            kCGImageSourceCreateThumbnailWithTransform: true,
            kCGImageSourceThumbnailMaxPixelSize: 64
        ]
        guard let cgImage = CGImageSourceCreateThumbnailAtIndex(source, 0, options as CFDictionary) else {
            throw DHashError.decodeFailed
        }

        let bytesPerRow = width
        let pixelCount = width * height
        var pixels = [UInt8](repeating: 0, count: pixelCount)
        let colorSpace = CGColorSpaceCreateDeviceGray()

        // Keep CGContext + draw + read all inside the closure: the buffer pointer is only
        // guaranteed to be valid here. CGContext holds the pointer internally, so reads
        // and writes through it must happen before the closure returns.
        return try pixels.withUnsafeMutableBufferPointer { buf -> Data in
            guard let context = CGContext(
                data: buf.baseAddress,
                width: width,
                height: height,
                bitsPerComponent: 8,
                bytesPerRow: bytesPerRow,
                space: colorSpace,
                bitmapInfo: CGImageAlphaInfo.none.rawValue
            ) else {
                throw DHashError.contextCreationFailed
            }
            context.interpolationQuality = .medium
            context.draw(cgImage, in: CGRect(x: 0, y: 0, width: width, height: height))

            // 8 rows × 8 differences (between adjacent columns) = 64 bits
            var bits: UInt64 = 0
            var bitIndex: UInt64 = 0
            for y in 0..<height {
                for x in 0..<(width - 1) {
                    if buf[y * width + x] > buf[y * width + x + 1] {
                        bits |= (1 << bitIndex)
                    }
                    bitIndex += 1
                }
            }
            var be = bits.bigEndian
            return Data(bytes: &be, count: MemoryLayout<UInt64>.size)
        }
    }

    static func hammingDistance(_ a: Data, _ b: Data) -> Int {
        guard a.count == b.count, a.count == 8 else { return Int.max }
        var x: UInt64 = 0
        var y: UInt64 = 0
        a.withUnsafeBytes { x = $0.load(as: UInt64.self) }
        b.withUnsafeBytes { y = $0.load(as: UInt64.self) }
        return (x ^ y).nonzeroBitCount
    }
}
