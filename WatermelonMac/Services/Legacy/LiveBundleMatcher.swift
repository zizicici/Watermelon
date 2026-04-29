import Foundation

struct LegacyMatcherOptions {
    var livePhotoTimeWindow: TimeInterval = 2.0
}

struct LegacyMatchedBundleSpec {
    let kind: LegacyBundleKind
    let creationDate: Date?
    let timestampSource: LegacyTimestampSource
    let components: [(role: Int, slot: Int, candidate: LegacyFileCandidate)]
}

final class LiveBundleMatcher {
    let options: LegacyMatcherOptions

    init(options: LegacyMatcherOptions = .init()) {
        self.options = options
    }

    func match(candidates: [LegacyFileCandidate]) -> [LegacyMatchedBundleSpec] {
        let groupedByStem = Dictionary(grouping: candidates) { $0.sanitizedStem.lowercased() }
        var bundles: [LegacyMatchedBundleSpec] = []

        for (_, files) in groupedByStem {
            if let pair = matchLivePhoto(files: files) {
                bundles.append(pair)
                continue
            }
            // No live-photo pairing — emit each file as its own bundle.
            for file in files {
                bundles.append(makeStandaloneBundle(file: file))
            }
        }

        return bundles.sorted { lhs, rhs in
            (lhs.creationDate ?? .distantPast) < (rhs.creationDate ?? .distantPast)
        }
    }

    private func matchLivePhoto(files: [LegacyFileCandidate]) -> LegacyMatchedBundleSpec? {
        guard files.count == 2 else { return nil }
        let images = files.filter { $0.kind == .image }
        let videos = files.filter { $0.kind == .video && $0.lowercasedExtension == "mov" }
        guard images.count == 1, videos.count == 1 else { return nil }
        let image = images[0]
        let video = videos[0]
        guard let imgT = image.timestamp, let vidT = video.timestamp else { return nil }
        guard abs(imgT.timeIntervalSince(vidT)) <= options.livePhotoTimeWindow else { return nil }

        return LegacyMatchedBundleSpec(
            kind: .livePhoto,
            creationDate: imgT,
            timestampSource: image.timestampSource,
            components: [
                (role: ResourceTypeCode.photo, slot: 0, candidate: image),
                (role: ResourceTypeCode.pairedVideo, slot: 0, candidate: video)
            ]
        )
    }

    private func makeStandaloneBundle(file: LegacyFileCandidate) -> LegacyMatchedBundleSpec {
        let role: Int
        let kind: LegacyBundleKind
        switch file.kind {
        case .image:
            role = ResourceTypeCode.photo
            kind = .photo
        case .video:
            role = ResourceTypeCode.video
            kind = .video
        }
        return LegacyMatchedBundleSpec(
            kind: kind,
            creationDate: file.timestamp,
            timestampSource: file.timestampSource,
            components: [(role: role, slot: 0, candidate: file)]
        )
    }
}
