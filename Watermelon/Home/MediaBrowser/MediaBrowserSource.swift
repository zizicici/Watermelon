import Photos
import UIKit
import os.log

private let mediaBrowserLoadLog = Logger(subsystem: "com.zizicici.watermelon", category: "MediaBrowserLoad")

enum MediaBrowserLoadTrace {
    struct Context: Sendable {
        let id: String
        let mode: MediaBrowserMode
        let startedAt: CFAbsoluteTime
    }

    @TaskLocal static var context: Context?

    static func makeContext(mode: MediaBrowserMode) -> Context {
        Context(
            id: String(UUID().uuidString.prefix(8)),
            mode: mode,
            startedAt: CFAbsoluteTimeGetCurrent()
        )
    }

    static func emit(
        _ stage: String,
        context explicitContext: Context? = context,
        startedAt: CFAbsoluteTime? = nil,
        details: @autoclosure () -> String = ""
    ) {
        guard let context = explicitContext else { return }
        let details = details()
        let now = CFAbsoluteTimeGetCurrent()
        let elapsedMs = (now - context.startedAt) * 1_000
        let segmentMs = startedAt.map { (now - $0) * 1_000 }
        let segmentText = segmentMs.map { String(format: " segmentMs=%.1f", $0) } ?? ""
        let detailText = details.isEmpty ? "" : " \(details)"
        let message = "[MediaBrowserLoad] id=\(context.id) mode=\(context.mode.diagnosticName) stage=\(stage) elapsedMs=\(String(format: "%.1f", elapsedMs))\(segmentText)\(detailText)"
        mediaBrowserLoadLog.info("\(message, privacy: .public)")
    }
}

private extension MediaBrowserMode {
    var diagnosticName: String {
        switch self {
        case .local: return "local"
        case .remote: return "remote"
        case .merged: return "merged"
        }
    }
}

struct MediaBrowserMonthMemo {
    private let calendar: Calendar
    private var cachedRange: Range<Date>?
    private var cachedMonth: LibraryMonthKey?
    private(set) var calculationCount = 0

    init(calendar: Calendar) {
        self.calendar = calendar
    }

    mutating func month(for date: Date) -> LibraryMonthKey {
        if let cachedRange, cachedRange.contains(date), let cachedMonth {
            return cachedMonth
        }
        calculationCount += 1
        let month = LibraryMonthKey.from(date: date, calendar: calendar)
        if let interval = calendar.dateInterval(of: .month, for: date) {
            cachedRange = interval.start ..< interval.end
            cachedMonth = month
        } else {
            cachedRange = nil
            cachedMonth = nil
        }
        return month
    }
}

// A configurable data source for the unified media browser. Local, Remote, and Merged implementations
// feed the same grid + full-screen viewer. Materializers follow a local-first strategy where possible.
protocol MediaBrowserSource: AnyObject {
    var mode: MediaBrowserMode { get }

    // Loads any indexes needed (e.g. the remote fingerprint→localIdentifier map) before first use.
    func prepare() async

    // Per-month, date-descending sections.
    func loadSections() async -> [MediaBrowserSection]

    // Grid thumbnail (small, cached). Nil → the cell shows a placeholder.
    func thumbnail(for item: MediaBrowserItem) async -> UIImage?

    // Full still image for a photo (already downsampled for display / bounded memory).
    func photoImage(for item: MediaBrowserItem) async -> UIImage?

    // Native Live Photo, or nil to fall back to a still + play button.
    func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto?

    // Playable video file (local PHAsset file or downloaded original).
    func video(for item: MediaBrowserItem) async -> MaterializedVideo?

    // Actions available for an item (rendered in the viewer chrome).
    func actions(for item: MediaBrowserItem) -> [MediaBrowserActionKind]

    // Activity-sheet items (UIImage / file URL) for the Share action.
    func shareItems(for item: MediaBrowserItem) async -> [Any]

    func shutdown() async
}

extension MediaBrowserSource {
    func actions(for item: MediaBrowserItem) -> [MediaBrowserActionKind] { [] }
    func shutdown() async {}

    // Default share: the video file for videos, otherwise the still image.
    func shareItems(for item: MediaBrowserItem) async -> [Any] {
        if item.isVideo, let video = await video(for: item) { return [video.url] }
        if let image = await photoImage(for: item) { return [image] }
        return []
    }
}

enum MediaDisplay {
    // Cap decoded still-image size to bound memory in the pager (still ample for on-screen zoom).
    static let maxPixel = 3000
}
