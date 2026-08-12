import AVFoundation
import Foundation
import ImageIO

struct LeftoverDownloadedCheck: Sendable {
    let file: LeftoverFile
    let status: LeftoverHashCheckStatus
    let inspection: LeftoverFileInspection

    func replacingLocalPresence(_ presence: LeftoverLocalPresence) -> LeftoverDownloadedCheck {
        LeftoverDownloadedCheck(
            file: file,
            status: status,
            inspection: LeftoverFileInspection(
                contentHash: inspection.contentHash,
                actualSize: inspection.actualSize,
                mediaKind: inspection.mediaKind,
                livePhotoContentIdentifier: inspection.livePhotoContentIdentifier,
                mediaCreationDateMs: inspection.mediaCreationDateMs,
                localPresence: presence
            )
        )
    }
}

enum LeftoverHashCheckMerger {
    static func merge(
        existing: [LeftoverDownloadedCheck],
        current: [LeftoverDownloadedCheck],
        failedPaths: Set<String>
    ) -> [LeftoverDownloadedCheck] {
        var byPath = Dictionary(
            existing.map { ($0.file.path, $0) },
            uniquingKeysWith: { _, new in new }
        )
        for check in current {
            byPath[check.file.path] = check
        }
        for path in failedPaths {
            byPath[path] = nil
        }
        return Array(byPath.values)
    }
}

enum LeftoverMediaInspector {
    static func inspect(_ url: URL) async -> (
        kind: LeftoverMediaKind,
        livePhotoContentIdentifier: String?,
        creationDateMs: Int64?
    ) {
        let options = [kCGImageSourceShouldCache: false] as CFDictionary
        if let source = CGImageSourceCreateWithURL(url as CFURL, options),
           CGImageSourceGetCount(source) > 0 {
            let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [String: Any]
            let apple = properties?[kCGImagePropertyMakerAppleDictionary as String] as? [String: Any]
            let identifier = normalizedIdentifier(apple?["17"])
            return (.image, identifier, imageCreationDateMs(properties))
        }

        let asset = AVURLAsset(url: url)
        guard let tracks = try? await asset.loadTracks(withMediaType: .video), !tracks.isEmpty else {
            return (.unsupported, nil, nil)
        }
        var creationDateMs: Int64?
        if let item = try? await asset.load(.creationDate),
           let date = try? await item.load(.dateValue) {
            creationDateMs = LibraryCreationDate.optionalMilliseconds(date)
        }
        let metadata = (try? await asset.loadMetadata(for: .quickTimeMetadata)) ?? []
        for item in metadata where item.identifier == .quickTimeMetadataContentIdentifier {
            if let value = try? await item.load(.stringValue),
               let identifier = normalizedIdentifier(value) {
                return (.video, identifier, creationDateMs)
            }
        }
        return (.video, nil, creationDateMs)
    }

    private static func normalizedIdentifier(_ value: Any?) -> String? {
        guard let text = value as? String else { return nil }
        let normalized = text.trimmingCharacters(in: .whitespacesAndNewlines)
        return normalized.isEmpty ? nil : normalized
    }

    private static func imageCreationDateMs(_ properties: [String: Any]?) -> Int64? {
        let exif = properties?[kCGImagePropertyExifDictionary as String] as? [String: Any]
        let pairs = [
            (
                exif?[kCGImagePropertyExifDateTimeOriginal as String] as? String,
                exif?["OffsetTimeOriginal"] as? String
            ),
            (
                exif?[kCGImagePropertyExifDateTimeDigitized as String] as? String,
                exif?["OffsetTimeDigitized"] as? String
            )
        ]
        let tiff = properties?[kCGImagePropertyTIFFDictionary as String] as? [String: Any]
        let date = pairs.lazy.compactMap(parseImageDate).first
            ?? parseImageDate((tiff?[kCGImagePropertyTIFFDateTime as String] as? String, nil))
        return LibraryCreationDate.optionalMilliseconds(date)
    }

    private static func parseImageDate(_ pair: (String?, String?)) -> Date? {
        guard let value = pair.0 else { return nil }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.calendar = Calendar(identifier: .gregorian)
        if let offset = pair.1, !offset.isEmpty {
            formatter.dateFormat = "yyyy:MM:dd HH:mm:ssXXXXX"
            if let date = formatter.date(from: value + offset) { return date }
        }
        formatter.timeZone = .current
        formatter.dateFormat = "yyyy:MM:dd HH:mm:ss"
        return formatter.date(from: value)
    }

}

enum LeftoverResidualHashCounter {
    struct Result: Sendable, Equatable {
        let hashByPath: [String: Data]
        let counts: [Data: Int]
    }

    static func count(
        targetHashes: Set<Data>,
        targetSizes: Set<Int64>,
        leftovers: [LeftoverFile],
        client: any RemoteStorageClientProtocol
    ) async throws -> Result {
        var hashByPath: [String: Data] = [:]
        var counts: [Data: Int] = [:]
        for file in leftovers where file.size <= 0 || targetSizes.contains(file.size) {
            try Task.checkCancellation()
            let pathExtension = (file.fileName as NSString).pathExtension
            let suffix = pathExtension.isEmpty ? "" : ".\(pathExtension)"
            let localURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("leftover-unique-\(UUID().uuidString)\(suffix)")
            do {
                try await client.download(
                    remotePath: file.path,
                    localURL: localURL,
                    expectedSize: file.size > 0 ? file.size : nil,
                    onProgress: nil
                )
                let hashed = try AssetProcessor.contentHashAndSize(of: localURL)
                guard file.size <= 0 || hashed.size == file.size else {
                    throw CocoaError(.fileReadCorruptFile)
                }
                hashByPath[file.path] = hashed.hash
                if targetHashes.contains(hashed.hash) {
                    counts[hashed.hash, default: 0] += 1
                }
            } catch {
                try? FileManager.default.removeItem(at: localURL)
                throw error
            }
            try? FileManager.default.removeItem(at: localURL)
        }
        return Result(hashByPath: hashByPath, counts: counts)
    }
}

enum LeftoverAdoptionCandidateMatcher {
    private struct PairGroupKey: Hashable {
        let month: LibraryMonthKey
        let value: String
    }

    static func makeCandidates(
        from checks: [LeftoverDownloadedCheck]
    ) -> [LeftoverAdoptionCandidate] {
        let remotelyUnique = checks.filter {
            guard $0.inspection.localPresence == .absent else { return false }
            if case .noMatch = $0.status {
                return $0.inspection.mediaKind != .unsupported
            }
            return false
        }
        let hashCounts = Dictionary(grouping: remotelyUnique, by: { $0.inspection.contentHash })
            .mapValues(\.count)
        let eligible = remotelyUnique.filter { hashCounts[$0.inspection.contentHash] == 1 }
        let identified = eligible.filter { $0.inspection.livePhotoContentIdentifier != nil }
        let identifierGroups = Dictionary(grouping: identified) {
            PairGroupKey(
                month: $0.file.month,
                value: $0.inspection.livePhotoContentIdentifier!
            )
        }
        var candidates: [LeftoverAdoptionCandidate] = []
        var consumedPaths = Set<String>()
        for group in identifierGroups.values {
            let photos = group.filter { $0.inspection.mediaKind == .image }
            let videos = group.filter { $0.inspection.mediaKind == .video }
            guard photos.count == 1, videos.count == 1 else { continue }
            let photo = photos[0]
            let video = videos[0]
            let creationDateMs = preferredCreationDateMs(for: photo)
            candidates.append(LeftoverAdoptionCandidate(
                resources: [
                    resource(
                        from: photo,
                        role: ResourceTypeCode.photo,
                        creationDateMs: creationDateMs
                    ),
                    resource(
                        from: video,
                        role: ResourceTypeCode.pairedVideo,
                        creationDateMs: creationDateMs
                    )
                ]
            ))
            consumedPaths.insert(photo.file.path)
            consumedPaths.insert(video.file.path)
        }

        for check in eligible.sorted(by: { $0.file.path < $1.file.path }) {
            guard !consumedPaths.contains(check.file.path),
                  check.inspection.livePhotoContentIdentifier == nil else { continue }
            switch check.inspection.mediaKind {
            case .image:
                candidates.append(LeftoverAdoptionCandidate(
                    resources: [resource(
                        from: check,
                        role: ResourceTypeCode.photo,
                        creationDateMs: preferredCreationDateMs(for: check)
                    )]
                ))
            case .video:
                candidates.append(LeftoverAdoptionCandidate(
                    resources: [resource(
                        from: check,
                        role: ResourceTypeCode.video,
                        creationDateMs: preferredCreationDateMs(for: check)
                    )]
                ))
            case .unsupported:
                break
            }
        }
        return candidates
    }

    private static func resource(
        from check: LeftoverDownloadedCheck,
        role: Int,
        creationDateMs: Int64?
    ) -> LeftoverAdoptionResource {
        LeftoverAdoptionResource(
            file: check.file,
            contentHash: check.inspection.contentHash,
            fileSize: check.inspection.actualSize,
            role: role,
            creationDateMs: creationDateMs
        )
    }

    private static func preferredCreationDateMs(for check: LeftoverDownloadedCheck) -> Int64? {
        check.inspection.mediaCreationDateMs
            ?? check.file.modificationDateMs
    }

}
