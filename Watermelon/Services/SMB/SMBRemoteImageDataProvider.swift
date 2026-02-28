import Foundation
import Kingfisher

struct SMBRemoteImageDataProvider: ImageDataProvider {
    let cacheKey: String

    private let profile: ServerProfileRecord
    private let password: String
    private let remoteAbsolutePath: String
    private let maxPixelSize: CGFloat
    private let thumbnailService: RemoteThumbnailService

    init(
        profile: ServerProfileRecord,
        password: String,
        remoteAbsolutePath: String,
        maxPixelSize: CGFloat,
        thumbnailService: RemoteThumbnailService
    ) {
        self.profile = profile
        self.password = password
        self.remoteAbsolutePath = remoteAbsolutePath
        self.maxPixelSize = max(1, maxPixelSize)
        self.thumbnailService = thumbnailService

        let pixelKey = Int(self.maxPixelSize.rounded())
        self.cacheKey = [
            profile.storageProfile.identityKey,
            remoteAbsolutePath,
            "px:\(pixelKey)"
        ].joined(separator: "|")
    }

    func data(handler: @escaping @Sendable (Result<Data, any Error>) -> Void) {
        Task {
            do {
                let data = try await thumbnailService.loadThumbnailData(
                    profile: profile,
                    password: password,
                    remoteAbsolutePath: remoteAbsolutePath,
                    maxPixelSize: maxPixelSize
                )
                handler(.success(data))
            } catch {
                handler(.failure(error))
            }
        }
    }
}
