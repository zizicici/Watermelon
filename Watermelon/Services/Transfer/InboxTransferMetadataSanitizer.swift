import AVFoundation
import CoreMedia
import Foundation
import ImageIO
import Photos

enum InboxTransferMetadataSanitizerError: LocalizedError {
    case unsupportedFormat
    case locationMetadataRemains
    case writeFailed(Error?)

    var errorDescription: String? {
        switch self {
        case .unsupportedFormat:
            return String(localized: "transfer.error.locationRemovalUnsupported")
        case .locationMetadataRemains:
            return String(localized: "transfer.error.locationRemovalIncomplete")
        case .writeFailed(let error):
            return error?.localizedDescription ?? String(localized: "transfer.error.locationRemovalFailed")
        }
    }
}

struct InboxTransferSanitizedResource {
    let url: URL
    let filenameExtension: String?
}

enum InboxTransferMetadataSanitizer {
    static func removingLocationMetadata(
        from sourceURL: URL,
        resourceType: PHAssetResourceType
    ) async throws -> InboxTransferSanitizedResource {
        try Task.checkCancellation()
        let role = PhotoLibraryService.resourceTypeCode(resourceType)
        if ResourceRole.isPhotoSide(role) {
            let url = try await Task.detached {
                try removeImageLocationMetadata(from: sourceURL)
            }.value
            return InboxTransferSanitizedResource(
                url: url,
                filenameExtension: normalizedExtension(sourceURL.pathExtension)
            )
        }
        if ResourceRole.isVideoSide(role) {
            return try await removeVideoLocationMetadata(from: sourceURL)
        }
        throw InboxTransferMetadataSanitizerError.unsupportedFormat
    }

    private static func removeImageLocationMetadata(from sourceURL: URL) throws -> URL {
        guard let source = CGImageSourceCreateWithURL(sourceURL as CFURL, nil),
              let type = CGImageSourceGetType(source) else {
            throw InboxTransferMetadataSanitizerError.unsupportedFormat
        }
        let destinationURL = temporaryURL(matching: sourceURL)
        guard let destination = CGImageDestinationCreateWithURL(
            destinationURL as CFURL,
            type,
            CGImageSourceGetCount(source),
            nil
        ) else {
            throw InboxTransferMetadataSanitizerError.unsupportedFormat
        }

        var copyError: Unmanaged<CFError>?
        let options = [kCGImageMetadataShouldExcludeGPS: true] as CFDictionary
        guard CGImageDestinationCopyImageSource(destination, source, options, &copyError) else {
            try? FileManager.default.removeItem(at: destinationURL)
            let error = copyError?.takeRetainedValue() as Error?
            throw InboxTransferMetadataSanitizerError.writeFailed(error)
        }
        try Task.checkCancellation()
        return destinationURL
    }

    private static func removeVideoLocationMetadata(
        from sourceURL: URL
    ) async throws -> InboxTransferSanitizedResource {
        let asset = AVURLAsset(url: sourceURL)
        guard let exporter = AVAssetExportSession(
            asset: asset,
            presetName: AVAssetExportPresetPassthrough
        ), let output = videoOutput(
            sourceExtension: sourceURL.pathExtension,
            supported: exporter.supportedFileTypes
        ) else {
            throw InboxTransferMetadataSanitizerError.unsupportedFormat
        }
        guard try await !containsDisallowedTimedMetadata(in: asset) else {
            throw InboxTransferMetadataSanitizerError.unsupportedFormat
        }

        let metadataFormats = try await asset.load(.availableMetadataFormats)
        var retainedMetadata: [AVMetadataItem] = []
        for format in metadataFormats {
            let items = try await asset.loadMetadata(for: format)
            retainedMetadata.append(contentsOf: items.filter { !isLocationMetadata($0) })
        }
        exporter.metadata = retainedMetadata

        let destinationURL = temporaryURL(fileExtension: output.filenameExtension)
        do {
            try await exporter.export(to: destinationURL, as: output.fileType)
            try Task.checkCancellation()
            guard try await !containsLocationMetadata(in: destinationURL) else {
                throw InboxTransferMetadataSanitizerError.locationMetadataRemains
            }
            return InboxTransferSanitizedResource(
                url: destinationURL,
                filenameExtension: output.filenameExtension
            )
        } catch {
            try? FileManager.default.removeItem(at: destinationURL)
            throw error
        }
    }

    private static func containsLocationMetadata(in url: URL) async throws -> Bool {
        let asset = AVURLAsset(url: url)
        if try await containsDisallowedTimedMetadata(in: asset) {
            return true
        }
        for format in try await asset.load(.availableMetadataFormats) {
            if try await asset.loadMetadata(for: format).contains(where: isLocationMetadata) {
                return true
            }
        }
        return false
    }

    private static func containsDisallowedTimedMetadata(in asset: AVAsset) async throws -> Bool {
        for track in try await asset.loadTracks(withMediaType: .metadata) {
            for description in try await track.load(.formatDescriptions) {
                guard let identifiers = CMMetadataFormatDescriptionGetIdentifiers(description) as? [String],
                      timedMetadataIdentifiersAreAllowed(identifiers) else {
                    return true
                }
            }
        }
        return false
    }

    static func timedMetadataIdentifiersAreAllowed(_ identifiers: [String]) -> Bool {
        !identifiers.isEmpty && identifiers.allSatisfy {
            $0 == "mdta/com.apple.quicktime.still-image-time"
        }
    }

    private static func isLocationMetadata(_ item: AVMetadataItem) -> Bool {
        guard let identifier = item.identifier else { return false }
        if locationIdentifiers.contains(identifier) { return true }
        let rawValue = identifier.rawValue.lowercased()
        return rawValue.contains("location") || rawValue.contains("iso6709") || rawValue.hasSuffix("xyz")
    }

    private static let locationIdentifiers: Set<AVMetadataIdentifier> = [
        .commonIdentifierLocation,
        .quickTimeUserDataLocationISO6709,
        .quickTimeMetadataLocationISO6709,
        .quickTimeMetadataLocationName,
        .quickTimeMetadataLocationBody,
        .quickTimeMetadataLocationNote,
        .quickTimeMetadataLocationRole,
        .quickTimeMetadataLocationDate,
        .quickTimeMetadataLocationHorizontalAccuracyInMeters,
        .identifier3GPUserDataLocation,
    ]

    struct VideoOutput: Equatable {
        let fileType: AVFileType
        let filenameExtension: String
    }

    static func videoOutput(
        sourceExtension: String,
        supported: [AVFileType]
    ) -> VideoOutput? {
        let candidates: [VideoOutput] = switch sourceExtension.lowercased() {
        case "mp4": [VideoOutput(fileType: .mp4, filenameExtension: "mp4"),
                     VideoOutput(fileType: .mov, filenameExtension: "mov")]
        case "m4v": [VideoOutput(fileType: .m4v, filenameExtension: "m4v"),
                     VideoOutput(fileType: .mov, filenameExtension: "mov")]
        case "mov": [VideoOutput(fileType: .mov, filenameExtension: "mov")]
        default: [VideoOutput(fileType: .mov, filenameExtension: "mov"),
                  VideoOutput(fileType: .mp4, filenameExtension: "mp4"),
                  VideoOutput(fileType: .m4v, filenameExtension: "m4v")]
        }
        return candidates.first { supported.contains($0.fileType) }
    }

    private static func temporaryURL(matching sourceURL: URL) -> URL {
        temporaryURL(fileExtension: normalizedExtension(sourceURL.pathExtension))
    }

    private static func temporaryURL(fileExtension: String?) -> URL {
        return FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString + (fileExtension.map { ".\($0)" } ?? "")
        )
    }

    private static func normalizedExtension(_ value: String) -> String? {
        let normalized = value.lowercased()
        return normalized.isEmpty ? nil : normalized
    }
}
