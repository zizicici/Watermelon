import Foundation
import ImageIO
import UniformTypeIdentifiers

actor RemoteThumbnailService {
    private actor AsyncLimiter {
        private let limit: Int
        private var available: Int
        private var waiters: [CheckedContinuation<Void, Never>] = []

        init(limit: Int) {
            self.limit = max(limit, 1)
            self.available = max(limit, 1)
        }

        func acquire() async {
            if available > 0 {
                available -= 1
                return
            }
            await withCheckedContinuation { continuation in
                waiters.append(continuation)
            }
        }

        func release() {
            if !waiters.isEmpty {
                let waiter = waiters.removeFirst()
                waiter.resume()
                return
            }
            available = min(available + 1, limit)
        }
    }

    private let limiter = AsyncLimiter(limit: 3)
    private var client: AMSMB2Client?
    private var clientKey: String?

    deinit {
        let client = client
        Task {
            await client?.disconnect()
        }
    }

    func loadThumbnailData(
        profile: ServerProfileRecord,
        password: String,
        remoteAbsolutePath: String,
        maxPixelSize: CGFloat
    ) async throws -> Data {
        await limiter.acquire()
        do {
            try Task.checkCancellation()
            let client = try await resolvedClient(profile: profile, password: password)
            let tempURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("remote_preview_\(UUID().uuidString)")
            try? FileManager.default.removeItem(at: tempURL)
            defer { try? FileManager.default.removeItem(at: tempURL) }

            try await client.download(remotePath: remoteAbsolutePath, localURL: tempURL)
            try Task.checkCancellation()

            let data: Data
            if let thumbnailData = try Self.downsampleData(at: tempURL, maxPixelSize: maxPixelSize) {
                data = thumbnailData
            } else {
                data = try Data(contentsOf: tempURL, options: [.mappedIfSafe])
            }
            await limiter.release()
            return data
        } catch {
            await limiter.release()
            throw error
        }
    }

    func invalidate() async {
        await client?.disconnect()
        client = nil
        clientKey = nil
    }

    private func resolvedClient(profile: ServerProfileRecord, password: String) async throws -> AMSMB2Client {
        let key = "\(profile.host)|\(profile.port)|\(profile.shareName)|\(profile.basePath)|\(profile.username)|\(profile.domain ?? "")"
        if let client, clientKey == key {
            return client
        }

        await self.client?.disconnect()

        let nextClient = try AMSMB2Client(config: SMBServerConfig(
            host: profile.host,
            port: profile.port,
            shareName: profile.shareName,
            basePath: profile.basePath,
            username: profile.username,
            password: password,
            domain: profile.domain
        ))
        try await nextClient.connect()

        client = nextClient
        clientKey = key
        return nextClient
    }

    private static func downsampleData(at url: URL, maxPixelSize: CGFloat) throws -> Data? {
        let sourceOptions = [kCGImageSourceShouldCache: false] as CFDictionary
        guard let source = CGImageSourceCreateWithURL(url as CFURL, sourceOptions) else {
            return nil
        }

        let options = [
            kCGImageSourceCreateThumbnailFromImageAlways: true,
            kCGImageSourceShouldCacheImmediately: true,
            kCGImageSourceCreateThumbnailWithTransform: true,
            kCGImageSourceThumbnailMaxPixelSize: max(1, Int(maxPixelSize))
        ] as CFDictionary
        guard let cgImage = CGImageSourceCreateThumbnailAtIndex(source, 0, options) else {
            return nil
        }

        let outputData = NSMutableData()
        let destinationOptions = [kCGImageDestinationLossyCompressionQuality: 0.82] as CFDictionary
        guard let destination = CGImageDestinationCreateWithData(
            outputData,
            UTType.jpeg.identifier as CFString,
            1,
            nil
        ) else {
            return nil
        }
        CGImageDestinationAddImage(destination, cgImage, destinationOptions)
        guard CGImageDestinationFinalize(destination) else {
            return nil
        }
        return outputData as Data
    }
}
