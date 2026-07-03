import Photos
import UIKit

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
