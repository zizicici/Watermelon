import AVFoundation
import Foundation
import ImageIO

struct MediaTimestampResult {
    let date: Date?
    let source: LegacyTimestampSource
}

final class MediaTimestampReader {
    func read(url: URL, kind: LegacyMediaKind, fallbackMtime: Date?) async -> MediaTimestampResult {
        switch kind {
        case .image:
            if let date = readImageEXIFDate(url: url) {
                return MediaTimestampResult(date: date, source: .exif)
            }
        case .video:
            if let date = await readVideoCreationDate(url: url) {
                return MediaTimestampResult(date: date, source: .quickTime)
            }
        }
        if let mtime = fallbackMtime {
            return MediaTimestampResult(date: mtime, source: .mtime)
        }
        return MediaTimestampResult(date: nil, source: .unknown)
    }

    private func readImageEXIFDate(url: URL) -> Date? {
        guard let source = CGImageSourceCreateWithURL(url as CFURL, nil) else { return nil }
        guard let props = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any] else {
            return nil
        }

        let exif = props[kCGImagePropertyExifDictionary] as? [CFString: Any]
        let dateString = (exif?[kCGImagePropertyExifDateTimeOriginal] as? String)
            ?? (exif?[kCGImagePropertyExifDateTimeDigitized] as? String)
            ?? (props[kCGImagePropertyTIFFDictionary] as? [CFString: Any])?[kCGImagePropertyTIFFDateTime] as? String

        guard let dateString else { return nil }

        let offsetString = exif?[kCGImagePropertyExifOffsetTimeOriginal] as? String
            ?? exif?[kCGImagePropertyExifOffsetTimeDigitized] as? String

        return Self.parseExifDate(dateString, offsetString: offsetString)
    }

    private func readVideoCreationDate(url: URL) async -> Date? {
        let asset = AVURLAsset(url: url)
        do {
            let creation = try await asset.load(.creationDate)
            if let date = try await creation?.load(.dateValue) {
                return date
            }
        } catch {
            // fall through
        }
        return nil
    }

    private static let exifFormatterUTC: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy:MM:dd HH:mm:ss"
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(secondsFromGMT: 0)
        return f
    }()

    private static let exifFormatterLocal: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy:MM:dd HH:mm:ss"
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = .current
        return f
    }()

    private static func parseExifDate(_ raw: String, offsetString: String?) -> Date? {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }

        if let offsetString,
           let offset = parseOffset(offsetString) {
            let formatter = exifFormatterUTC
            formatter.timeZone = TimeZone(secondsFromGMT: offset)
            return formatter.date(from: trimmed)
        }
        return exifFormatterLocal.date(from: trimmed)
    }

    private static func parseOffset(_ raw: String) -> Int? {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.count >= 6 else { return nil }
        let sign: Int = trimmed.hasPrefix("-") ? -1 : 1
        let body = trimmed.dropFirst()
        let parts = body.split(separator: ":")
        guard parts.count == 2,
              let hours = Int(parts[0]),
              let minutes = Int(parts[1]) else { return nil }
        return sign * (hours * 3600 + minutes * 60)
    }
}
