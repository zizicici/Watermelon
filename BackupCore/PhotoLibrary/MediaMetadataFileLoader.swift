import AVFoundation
import ImageIO
import UniformTypeIdentifiers

enum MediaMetadataFileLoader {
    nonisolated static func imageDocument(
        at url: URL,
        summary: MediaMetadataSummary
    ) -> MediaMetadataDocument {
        let options = [kCGImageSourceShouldCache: false] as CFDictionary
        guard let source = CGImageSourceCreateWithURL(
            url as CFURL,
            options
        ),
        let rawProperties = CGImageSourceCopyPropertiesAtIndex(
            source,
            0,
            nil
        ),
        let properties = stringDictionary(rawProperties) else {
            return MediaMetadataParser.summaryDocument(summary)
        }
        let width = integer(
            properties[kCGImagePropertyPixelWidth as String]
        )
        let height = integer(
            properties[kCGImagePropertyPixelHeight as String]
        )
        let sourceType = CGImageSourceGetType(source).map {
            $0 as String
        }
        return MediaMetadataParser.document(
            imageProperties: properties,
            summary: summary.replacing(
                fileType: sourceType.flatMap(typeDescription),
                fileSize: fileSize(at: url),
                pixelWidth: width,
                pixelHeight: height
            )
        )
    }

    nonisolated static func videoDocument(
        at url: URL,
        summary: MediaMetadataSummary
    ) async -> MediaMetadataDocument {
        await videoDocument(
            asset: AVURLAsset(url: url),
            fileURL: url,
            summary: summary
        )
    }

    nonisolated static func videoDocument(
        asset: AVAsset,
        fileURL: URL?,
        summary: MediaMetadataSummary
    ) async -> MediaMetadataDocument {
        let duration = (try? await asset.load(.duration)).map(
            CMTimeGetSeconds
        )
        let tracks =
            (try? await asset.loadTracks(withMediaType: .video)) ?? []
        var width: Int?
        var height: Int?
        var rows: [MediaMetadataDocument.Row] = []
        if let track = tracks.first {
            let naturalSize = try? await track.load(.naturalSize)
            let transform = try? await track.load(.preferredTransform)
            if let naturalSize {
                let displayed = transform.map {
                    naturalSize.applying($0)
                } ?? naturalSize
                width = Int(abs(displayed.width).rounded())
                height = Int(abs(displayed.height).rounded())
            }
            if let frameRate = try? await track.load(
                .nominalFrameRate
            ),
            frameRate > 0 {
                let formatted = MediaMetadataParser.decimal(
                    Double(frameRate),
                    maximumFractionDigits: 3
                )
                rows.append(
                    .init(
                        label: "FrameRate",
                        value: "\(formatted) fps"
                    )
                )
            }
            if let dataRate = try? await track.load(
                .estimatedDataRate
            ),
            dataRate > 0 {
                let formatted = MediaMetadataParser.decimal(
                    Double(dataRate) / 1_000_000,
                    maximumFractionDigits: 2
                )
                rows.append(
                    .init(
                        label: "DataRate",
                        value: "\(formatted) Mbps"
                    )
                )
            }
        }
        let resolved = summary.replacing(
            fileSize: fileURL.flatMap(fileSize),
            pixelWidth: width,
            pixelHeight: height,
            duration: duration.flatMap {
                $0.isFinite && $0 > 0 ? $0 : nil
            }
        )
        return MediaMetadataParser.videoDocument(
            summary: resolved,
            technicalRows: rows
        )
    }

    nonisolated static func typeDescription(
        _ identifier: String
    ) -> String {
        guard let type = UTType(identifier) else {
            return identifier
        }
        if let ext = type.preferredFilenameExtension,
           !ext.isEmpty {
            return ext.uppercased()
        }
        return type.localizedDescription ?? identifier
    }

    nonisolated static func fileSize(at url: URL) -> Int64? {
        guard let value = try? url.resourceValues(
            forKeys: [.fileSizeKey]
        ).fileSize else {
            return nil
        }
        return Int64(value)
    }

    private nonisolated static func integer(_ value: Any?) -> Int? {
        if let number = value as? NSNumber {
            return number.intValue
        }
        return value as? Int
    }

    private nonisolated static func stringDictionary(
        _ dictionary: CFDictionary
    ) -> [String: Any]? {
        guard let dictionary = dictionary as NSDictionary? else {
            return nil
        }
        var result: [String: Any] = [:]
        result.reserveCapacity(dictionary.count)
        for (key, value) in dictionary {
            result[String(describing: key)] = value
        }
        return result
    }
}
