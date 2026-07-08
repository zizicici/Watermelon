import Foundation

// On-device persistent cache for media originals downloaded by the remote browser, so re-viewing a
// remote-only item doesn't re-download it. Content-addressed by a caller-supplied key (assetFingerprint
// hex for photos, hex + "-v" for the paired/standalone video — a Live Photo's photo and video share one
// fingerprint), sharded by the first byte (mirrors RemoteThumbnailPaths). Lives in Caches/ (the OS may
// reclaim it). Photos are always eligible; videos only when small (see the caller's per-entry cap).
// Local-present assets never reach here.
final class OriginalPhotoCache: @unchecked Sendable {
    static let shared = OriginalPhotoCache()

    // Standalone/Live videos are only persisted when at or below this size (they're otherwise streamed
    // to temp and dropped). Live Photo clips and short videos fit comfortably; large videos don't.
    static let videoCacheMaxEntryBytes: Int64 = 50 * 1024 * 1024

    private let lock = NSLock()
    private let root: URL

    init(root: URL? = nil) {
        if let root {
            self.root = root
        } else {
            let caches = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first
                ?? FileManager.default.temporaryDirectory
            self.root = caches.appendingPathComponent("RemoteOriginals", isDirectory: true)
        }
    }

    static func photoKey(fingerprintHex: String) -> String { fingerprintHex }
    static func videoKey(fingerprintHex: String) -> String { fingerprintHex + "-v" }

    private func location(forKey key: String) -> URL {
        let shard = key.count >= 2 ? String(key.prefix(2)) : "_"
        return root.appendingPathComponent(shard, isDirectory: true).appendingPathComponent(key)
    }

    // Returns the cached file and bumps its modification date so LRU eviction keeps hot entries.
    func url(forKey key: String) -> URL? {
        lock.withLock {
            let url = location(forKey: key)
            guard FileManager.default.fileExists(atPath: url.path) else { return nil }
            try? FileManager.default.setAttributes([.modificationDate: Date()], ofItemAtPath: url.path)
            return url
        }
    }

    // Moves a freshly downloaded original into the cache and returns its canonical URL. If an entry
    // already exists (concurrent download), keeps it and reports storedIncoming = false — the incoming
    // file is left in place (the caller may still need its verified bytes, e.g. when the resident entry
    // belongs to a same-fingerprint twin with a different manifest hash).
    @discardableResult
    func store(movingFrom tempURL: URL, forKey key: String) -> (url: URL, storedIncoming: Bool)? {
        lock.withLock {
            let dest = location(forKey: key)
            let fm = FileManager.default
            if fm.fileExists(atPath: dest.path) {
                return (dest, false)
            }
            do {
                try fm.createDirectory(at: dest.deletingLastPathComponent(), withIntermediateDirectories: true)
                try fm.moveItem(at: tempURL, to: dest)
                return (dest, true)
            } catch {
                return nil
            }
        }
    }

    func remove(forKey key: String) {
        lock.withLock {
            try? FileManager.default.removeItem(at: location(forKey: key))
        }
    }

    // Evicts least-recently-used entries (by modification date) until total size is within maxBytes.
    func enforceCap(maxBytes: Int64) {
        lock.withLock {
            let fm = FileManager.default
            let keys: [URLResourceKey] = [.isRegularFileKey, .fileSizeKey, .contentModificationDateKey]
            guard let enumerator = fm.enumerator(at: root, includingPropertiesForKeys: keys) else { return }
            var files: [(url: URL, size: Int64, date: Date)] = []
            var total: Int64 = 0
            for case let url as URL in enumerator {
                guard let values = try? url.resourceValues(forKeys: Set(keys)),
                      values.isRegularFile == true else { continue }
                let size = Int64(values.fileSize ?? 0)
                total += size
                files.append((url, size, values.contentModificationDate ?? .distantPast))
            }
            guard total > maxBytes else { return }
            for file in files.sorted(by: { $0.date < $1.date }) {
                guard total > maxBytes else { break }
                try? fm.removeItem(at: file.url)
                total -= file.size
            }
        }
    }

    func diskSizeBytes() -> Int64 {
        lock.withLock {
            let fm = FileManager.default
            let keys: [URLResourceKey] = [.isRegularFileKey, .fileSizeKey]
            guard let enumerator = fm.enumerator(at: root, includingPropertiesForKeys: keys) else { return 0 }
            var total: Int64 = 0
            for case let url as URL in enumerator {
                guard let values = try? url.resourceValues(forKeys: Set(keys)),
                      values.isRegularFile == true else { continue }
                total += Int64(values.fileSize ?? 0)
            }
            return total
        }
    }

    func clear() {
        lock.withLock {
            try? FileManager.default.removeItem(at: root)
        }
    }
}
