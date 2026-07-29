import AVFoundation
import CoreFoundation
import ImageIO
import Photos
import UniformTypeIdentifiers

struct MediaMetadataDocument: Equatable, Sendable {
    enum DetailLevel: Equatable, Sendable {
        case summary
        case original
    }

    struct Section: Equatable, Sendable {
        let title: String
        let rows: [Row]
    }

    struct Row: Equatable, Sendable {
        let label: String
        let value: String
    }

    let sections: [Section]
    let detailLevel: DetailLevel

    var isSummaryOnly: Bool {
        detailLevel == .summary
    }
}

struct MediaMetadataSummary: Equatable, Sendable {
    let fileName: String?
    let fileType: String?
    let fileSize: Int64?
    let pixelWidth: Int?
    let pixelHeight: Int?
    let creationDate: Date?
    let duration: TimeInterval?
    let latitude: Double?
    let longitude: Double?
    let altitude: Double?
    let mediaType: String

    func replacing(
        fileType: String? = nil,
        fileSize: Int64? = nil,
        pixelWidth: Int? = nil,
        pixelHeight: Int? = nil,
        duration: TimeInterval? = nil
    ) -> MediaMetadataSummary {
        MediaMetadataSummary(
            fileName: fileName,
            fileType: self.fileType ?? fileType,
            fileSize: self.fileSize ?? fileSize,
            pixelWidth: self.pixelWidth ?? pixelWidth,
            pixelHeight: self.pixelHeight ?? pixelHeight,
            creationDate: creationDate,
            duration: self.duration ?? duration,
            latitude: latitude,
            longitude: longitude,
            altitude: altitude,
            mediaType: mediaType
        )
    }
}

enum MediaMetadataParser {
    private static let orderedImageGroups: [(key: String, title: String)] = [
        (kCGImagePropertyExifDictionary as String, "EXIF"),
        (kCGImagePropertyExifAuxDictionary as String, "EXIF Auxiliary"),
        (kCGImagePropertyTIFFDictionary as String, "TIFF"),
        (kCGImagePropertyGPSDictionary as String, "GPS"),
        (kCGImagePropertyIPTCDictionary as String, "IPTC"),
        (kCGImagePropertyJFIFDictionary as String, "JFIF"),
        (kCGImagePropertyPNGDictionary as String, "PNG"),
        (kCGImagePropertyHEICSDictionary as String, "HEIF"),
        (kCGImagePropertyMakerAppleDictionary as String, "Apple"),
    ]

    nonisolated static func document(
        imageProperties: [String: Any],
        summary: MediaMetadataSummary
    ) -> MediaMetadataDocument {
        var sections = [summarySection(summary)]
        var consumedKeys = Set<String>()

        for group in orderedImageGroups {
            guard let dictionary = dictionary(imageProperties[group.key]) else { continue }
            consumedKeys.insert(group.key)
            let rows = flattenedRows(dictionary)
            if !rows.isEmpty {
                sections.append(.init(title: group.title, rows: rows))
            }
        }

        var imageRows: [MediaMetadataDocument.Row] = []
        var extraSections: [MediaMetadataDocument.Section] = []
        for key in imageProperties.keys.sorted(by: technicalOrder) where !consumedKeys.contains(key) {
            if let dictionary = dictionary(imageProperties[key]) {
                let rows = flattenedRows(dictionary)
                if !rows.isEmpty {
                    extraSections.append(.init(title: cleanedSectionTitle(key), rows: rows))
                }
            } else if let value = formattedValue(imageProperties[key], key: key) {
                imageRows.append(.init(label: key, value: value))
            }
        }
        if !imageRows.isEmpty {
            sections.insert(.init(title: "Image", rows: imageRows), at: min(1, sections.count))
        }
        sections.append(contentsOf: extraSections.sorted { technicalOrder($0.title, $1.title) })
        return MediaMetadataDocument(sections: sections, detailLevel: .original)
    }

    nonisolated static func summaryDocument(_ summary: MediaMetadataSummary) -> MediaMetadataDocument {
        MediaMetadataDocument(sections: [summarySection(summary)], detailLevel: .summary)
    }

    nonisolated static func videoDocument(
        summary: MediaMetadataSummary,
        technicalRows: [MediaMetadataDocument.Row]
    ) -> MediaMetadataDocument {
        var sections = [summarySection(summary)]
        if !technicalRows.isEmpty {
            sections.append(.init(title: "Video", rows: technicalRows))
        }
        return MediaMetadataDocument(sections: sections, detailLevel: .original)
    }

    nonisolated static func flattenedRows(
        _ dictionary: [String: Any]
    ) -> [MediaMetadataDocument.Row] {
        var rows: [MediaMetadataDocument.Row] = []
        appendRows(from: dictionary, prefix: nil, to: &rows)
        return rows
    }

    private nonisolated static func appendRows(
        from values: [String: Any],
        prefix: String?,
        to rows: inout [MediaMetadataDocument.Row]
    ) {
        for key in values.keys.sorted(by: technicalOrder) {
            let path = prefix.map { "\($0).\(key)" } ?? key
            if let nested = dictionary(values[key]) {
                appendRows(from: nested, prefix: path, to: &rows)
            } else if let value = formattedValue(values[key], key: key) {
                rows.append(.init(label: path, value: value))
            }
        }
    }

    private nonisolated static func summarySection(
        _ summary: MediaMetadataSummary
    ) -> MediaMetadataDocument.Section {
        var rows: [MediaMetadataDocument.Row] = []
        if let fileName = summary.fileName, !fileName.isEmpty {
            rows.append(.init(label: "FileName", value: fileName))
        }
        rows.append(.init(label: "MediaType", value: summary.mediaType))
        if let fileType = summary.fileType, !fileType.isEmpty {
            rows.append(.init(label: "FileType", value: fileType))
        }
        if let fileSize = summary.fileSize, fileSize >= 0 {
            rows.append(.init(
                label: "FileSize",
                value: ByteCountFormatter.string(fromByteCount: fileSize, countStyle: .file)
            ))
        }
        if let width = summary.pixelWidth, let height = summary.pixelHeight, width > 0, height > 0 {
            rows.append(.init(label: "ImageSize", value: "\(width) × \(height)"))
        }
        if let creationDate = summary.creationDate {
            rows.append(.init(
                label: "CreateDate",
                value: creationDate.formatted(date: .abbreviated, time: .standard)
            ))
        }
        if let duration = summary.duration, duration > 0 {
            rows.append(.init(label: "Duration", value: durationText(duration)))
        }
        if let latitude = summary.latitude, let longitude = summary.longitude {
            rows.append(.init(
                label: "GPSPosition",
                value: "\(decimal(latitude, maximumFractionDigits: 6)), \(decimal(longitude, maximumFractionDigits: 6))"
            ))
        }
        if let altitude = summary.altitude {
            rows.append(.init(
                label: "GPSAltitude",
                value: "\(decimal(altitude, maximumFractionDigits: 1)) m"
            ))
        }
        return .init(title: "File", rows: rows)
    }

    private nonisolated static func formattedValue(_ value: Any?, key: String) -> String? {
        guard let value else { return nil }
        if let dictionary = dictionary(value) {
            let entries = flattenedRows(dictionary)
            return entries.map { "\($0.label): \($0.value)" }.joined(separator: "\n")
        }
        if let data = value as? Data {
            return ByteCountFormatter.string(fromByteCount: Int64(data.count), countStyle: .memory)
        }
        if let date = value as? Date {
            return date.formatted(date: .abbreviated, time: .standard)
        }
        if let array = value as? [Any] {
            let values = array.prefix(256).compactMap { formattedValue($0, key: key) }
            guard !values.isEmpty else { return nil }
            let suffix = array.count > 256 ? ", …" : ""
            return truncated(values.joined(separator: ", ") + suffix)
        }
        if let string = value as? String {
            guard !string.isEmpty else { return nil }
            return truncated(string)
        }
        if let number = value as? NSNumber {
            if CFGetTypeID(number) == CFBooleanGetTypeID() {
                return number.boolValue ? "Yes" : "No"
            }
            let numeric = number.doubleValue
            if key == (kCGImagePropertyExifExposureTime as String) {
                return exposureTime(numeric)
            }
            if key == (kCGImagePropertyExifFNumber as String) {
                return "ƒ/\(decimal(numeric, maximumFractionDigits: 2))"
            }
            if key == (kCGImagePropertyExifFocalLength as String)
                || key == (kCGImagePropertyExifFocalLenIn35mmFilm as String) {
                return "\(decimal(numeric, maximumFractionDigits: 1)) mm"
            }
            if key == (kCGImagePropertyExifExposureBiasValue as String) {
                let prefix = numeric > 0 ? "+" : ""
                return "\(prefix)\(decimal(numeric, maximumFractionDigits: 2)) EV"
            }
            return decimal(numeric, maximumFractionDigits: 6)
        }
        return truncated(String(describing: value))
    }

    private nonisolated static func dictionary(_ value: Any?) -> [String: Any]? {
        guard let dictionary = value as? NSDictionary else { return nil }
        var result: [String: Any] = [:]
        result.reserveCapacity(dictionary.count)
        for (key, value) in dictionary {
            result[String(describing: key)] = value
        }
        return result
    }

    private nonisolated static func cleanedSectionTitle(_ key: String) -> String {
        let cleaned = key.trimmingCharacters(in: CharacterSet(charactersIn: "{}"))
        return cleaned.isEmpty ? key : cleaned
    }

    private nonisolated static func exposureTime(_ seconds: Double) -> String {
        guard seconds > 0 else { return decimal(seconds, maximumFractionDigits: 6) }
        if seconds < 1 {
            let denominator = (1 / seconds).rounded()
            if denominator >= 2 {
                return "1/\(Int(denominator)) s"
            }
        }
        return "\(decimal(seconds, maximumFractionDigits: 4)) s"
    }

    nonisolated static func decimal(
        _ value: Double,
        maximumFractionDigits: Int
    ) -> String {
        let formatter = NumberFormatter()
        formatter.locale = .current
        formatter.numberStyle = .decimal
        formatter.minimumFractionDigits = 0
        formatter.maximumFractionDigits = maximumFractionDigits
        formatter.usesGroupingSeparator = false
        return formatter.string(from: NSNumber(value: value)) ?? String(value)
    }

    private nonisolated static func durationText(_ duration: TimeInterval) -> String {
        let totalSeconds = max(0, Int(duration.rounded()))
        let hours = totalSeconds / 3_600
        let minutes = (totalSeconds % 3_600) / 60
        let seconds = totalSeconds % 60
        if hours > 0 {
            return String(format: "%d:%02d:%02d", hours, minutes, seconds)
        }
        return String(format: "%d:%02d", minutes, seconds)
    }

    private nonisolated static func truncated(_ value: String) -> String {
        let limit = 4_096
        guard value.count > limit else { return value }
        return String(value.prefix(limit)) + "…"
    }

    private nonisolated static func technicalOrder(_ lhs: String, _ rhs: String) -> Bool {
        lhs.localizedStandardCompare(rhs) == .orderedAscending
    }
}

enum MediaMetadataLoader {
    private final class RequestState<Value, RequestID>: @unchecked Sendable {
        private let cancelRequest: (RequestID) -> Void
        private let lock = NSLock()
        private var continuation: CheckedContinuation<Value?, Never>?
        private var requestID: RequestID?
        private var completed = false

        init(cancelRequest: @escaping (RequestID) -> Void) {
            self.cancelRequest = cancelRequest
        }

        func bind(_ continuation: CheckedContinuation<Value?, Never>) -> Bool {
            lock.withLock {
                guard !completed else { return false }
                self.continuation = continuation
                return true
            }
        }

        func attach(_ requestID: RequestID) {
            let shouldCancel = lock.withLock {
                guard !completed else { return true }
                self.requestID = requestID
                return false
            }
            if shouldCancel {
                cancelRequest(requestID)
            }
        }

        func complete(_ value: Value?) {
            let continuation = lock.withLock {
                guard !completed else { return Optional<CheckedContinuation<Value?, Never>>.none }
                completed = true
                let captured = self.continuation
                self.continuation = nil
                requestID = nil
                return captured
            }
            continuation?.resume(returning: value)
        }

        func cancel() {
            let state = lock.withLock {
                guard !completed else {
                    return (Optional<RequestID>.none, Optional<CheckedContinuation<Value?, Never>>.none)
                }
                completed = true
                let captured = (requestID, continuation)
                requestID = nil
                continuation = nil
                return captured
            }
            if let requestID = state.0 {
                cancelRequest(requestID)
            }
            state.1?.resume(returning: nil)
        }
    }

    nonisolated static func localDocument(
        localIdentifier: String,
        item: MediaBrowserItem,
        allowNetworkAccess: Bool = true
    ) async -> MediaMetadataDocument? {
        guard let asset = PHAsset.fetchAssets(
            withLocalIdentifiers: [localIdentifier],
            options: nil
        ).firstObject else { return nil }

        let resources = PHAssetResource.assetResources(for: asset)
        let resource = preferredResource(in: resources, for: item.kind)
        let location = asset.location
        let summary = MediaMetadataSummary(
            fileName: resource.flatMap(originalFileName),
            fileType: resource.flatMap(resourceType),
            fileSize: resource.flatMap(resourceFileSize),
            pixelWidth: asset.pixelWidth > 0 ? asset.pixelWidth : nil,
            pixelHeight: asset.pixelHeight > 0 ? asset.pixelHeight : nil,
            creationDate: asset.creationDate,
            duration: asset.duration > 0 ? asset.duration : nil,
            latitude: location?.coordinate.latitude,
            longitude: location?.coordinate.longitude,
            altitude: location.flatMap { $0.verticalAccuracy >= 0 ? $0.altitude : nil },
            mediaType: mediaTypeName(item.kind)
        )

        guard let input = await requestContentEditingInput(
            for: asset,
            allowNetworkAccess: allowNetworkAccess
        ) else {
            return MediaMetadataParser.summaryDocument(summary)
        }
        if item.isVideo, let video = input.audiovisualAsset {
            return await videoDocument(asset: video, fileURL: (video as? AVURLAsset)?.url, summary: summary)
        }
        guard let url = input.fullSizeImageURL else {
            return MediaMetadataParser.summaryDocument(summary)
        }
        return imageDocument(at: url, summary: summary)
    }

    nonisolated static func remoteImageDocument(
        at url: URL,
        item: MediaBrowserItem,
        relativePath: String
    ) -> MediaMetadataDocument? {
        imageDocument(
            at: url,
            summary: remoteSummary(at: url, item: item, relativePath: relativePath)
        )
    }

    nonisolated static func remoteVideoDocument(
        at url: URL,
        item: MediaBrowserItem,
        relativePath: String
    ) async -> MediaMetadataDocument {
        let asset = AVURLAsset(url: url)
        return await videoDocument(
            asset: asset,
            fileURL: url,
            summary: remoteSummary(at: url, item: item, relativePath: relativePath)
        )
    }

    private nonisolated static func imageDocument(
        at url: URL,
        summary: MediaMetadataSummary
    ) -> MediaMetadataDocument? {
        let options = [kCGImageSourceShouldCache: false] as CFDictionary
        guard let source = CGImageSourceCreateWithURL(url as CFURL, options),
              let rawProperties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil),
              let properties = stringDictionary(rawProperties)
        else {
            return MediaMetadataParser.summaryDocument(summary)
        }
        let width = integer(properties[kCGImagePropertyPixelWidth as String])
        let height = integer(properties[kCGImagePropertyPixelHeight as String])
        let sourceType = CGImageSourceGetType(source).map { $0 as String }
        let typeName = sourceType.flatMap(typeDescription)
        let fileSize = localFileSize(url)
        return MediaMetadataParser.document(
            imageProperties: properties,
            summary: summary.replacing(
                fileType: typeName,
                fileSize: fileSize,
                pixelWidth: width,
                pixelHeight: height
            )
        )
    }

    private nonisolated static func videoDocument(
        asset: AVAsset,
        fileURL: URL?,
        summary: MediaMetadataSummary
    ) async -> MediaMetadataDocument {
        let duration = (try? await asset.load(.duration)).map(CMTimeGetSeconds)
        let tracks = (try? await asset.loadTracks(withMediaType: .video)) ?? []
        var width: Int?
        var height: Int?
        var technicalRows: [MediaMetadataDocument.Row] = []
        if let track = tracks.first {
            let naturalSize = try? await track.load(.naturalSize)
            let transform = try? await track.load(.preferredTransform)
            if let naturalSize {
                let displayed = transform.map { naturalSize.applying($0) } ?? naturalSize
                width = Int(abs(displayed.width).rounded())
                height = Int(abs(displayed.height).rounded())
            }
            if let frameRate = try? await track.load(.nominalFrameRate), frameRate > 0 {
                technicalRows.append(.init(
                    label: "FrameRate",
                    value: "\(MediaMetadataParser.decimal(Double(frameRate), maximumFractionDigits: 3)) fps"
                ))
            }
            if let dataRate = try? await track.load(.estimatedDataRate), dataRate > 0 {
                technicalRows.append(.init(
                    label: "DataRate",
                    value: "\(MediaMetadataParser.decimal(Double(dataRate) / 1_000_000, maximumFractionDigits: 2)) Mbps"
                ))
            }
        }
        let resolved = summary.replacing(
            fileSize: fileURL.flatMap(localFileSize),
            pixelWidth: width,
            pixelHeight: height,
            duration: duration.flatMap { $0.isFinite && $0 > 0 ? $0 : nil }
        )
        return MediaMetadataParser.videoDocument(summary: resolved, technicalRows: technicalRows)
    }

    private nonisolated static func requestContentEditingInput(
        for asset: PHAsset,
        allowNetworkAccess: Bool
    ) async -> PHContentEditingInput? {
        let options = PHContentEditingInputRequestOptions()
        options.isNetworkAccessAllowed = allowNetworkAccess
        let state = RequestState<PHContentEditingInput, PHContentEditingInputRequestID>(cancelRequest: { requestID in
            asset.cancelContentEditingInputRequest(requestID)
        })
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                guard !Task.isCancelled else {
                    state.cancel()
                    return
                }
                let requestID = asset.requestContentEditingInput(with: options) { input, _ in
                    state.complete(input)
                }
                state.attach(requestID)
            }
        } onCancel: {
            state.cancel()
        }
    }

    private nonisolated static func preferredResource(
        in resources: [PHAssetResource],
        for kind: AlbumMediaKind
    ) -> PHAssetResource? {
        let preferredTypes: [PHAssetResourceType]
        switch kind {
        case .photo, .livePhoto:
            preferredTypes = [.fullSizePhoto, .photo, .adjustmentBasePhoto, .alternatePhoto]
        case .video:
            preferredTypes = [.fullSizeVideo, .video, .adjustmentBaseVideo]
        }
        for type in preferredTypes {
            if let resource = resources.first(where: { $0.type == type }) {
                return resource
            }
        }
        return resources.first
    }

    private nonisolated static func originalFileName(_ resource: PHAssetResource) -> String? {
        resource.value(forKey: "originalFilename") as? String
    }

    private nonisolated static func resourceType(_ resource: PHAssetResource) -> String? {
        guard let identifier = resource.value(forKey: "uniformTypeIdentifier") as? String else {
            return nil
        }
        return typeDescription(identifier)
    }

    private nonisolated static func resourceFileSize(_ resource: PHAssetResource) -> Int64? {
        if let number = resource.value(forKey: "fileSize") as? NSNumber {
            return number.int64Value
        }
        return nil
    }

    private nonisolated static func remoteSummary(
        at url: URL,
        item: MediaBrowserItem,
        relativePath: String
    ) -> MediaMetadataSummary {
        let fileName = (relativePath as NSString).lastPathComponent
        let fileType = UTType(filenameExtension: (fileName as NSString).pathExtension)
            .flatMap { typeDescription($0.identifier) }
        return MediaMetadataSummary(
            fileName: fileName,
            fileType: fileType,
            fileSize: localFileSize(url),
            pixelWidth: nil,
            pixelHeight: nil,
            creationDate: Date(timeIntervalSince1970: Double(item.creationDateMs) / 1_000),
            duration: nil,
            latitude: nil,
            longitude: nil,
            altitude: nil,
            mediaType: mediaTypeName(item.kind)
        )
    }

    private nonisolated static func mediaTypeName(_ kind: AlbumMediaKind) -> String {
        switch kind {
        case .photo: return "Photo"
        case .video: return "Video"
        case .livePhoto: return "Live Photo"
        }
    }

    private nonisolated static func typeDescription(_ identifier: String) -> String {
        guard let type = UTType(identifier) else { return identifier }
        if let ext = type.preferredFilenameExtension, !ext.isEmpty {
            return ext.uppercased()
        }
        return type.localizedDescription ?? identifier
    }

    private nonisolated static func localFileSize(_ url: URL) -> Int64? {
        guard let value = try? url.resourceValues(forKeys: [.fileSizeKey]).fileSize else { return nil }
        return Int64(value)
    }

    private nonisolated static func integer(_ value: Any?) -> Int? {
        if let number = value as? NSNumber { return number.intValue }
        return value as? Int
    }

    private nonisolated static func stringDictionary(_ dictionary: CFDictionary) -> [String: Any]? {
        guard let dictionary = dictionary as NSDictionary? else { return nil }
        var result: [String: Any] = [:]
        result.reserveCapacity(dictionary.count)
        for (key, value) in dictionary {
            result[String(describing: key)] = value
        }
        return result
    }
}
