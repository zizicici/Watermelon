import Foundation
@preconcurrency import Photos

enum PhotoLibraryAccessState: Sendable, Equatable {
    case notDetermined
    case authorized
    case limited
    case denied
    case restricted
    case unknown

    init(_ status: PHAuthorizationStatus) {
        switch status {
        case .notDetermined:
            self = .notDetermined
        case .authorized:
            self = .authorized
        case .limited:
            self = .limited
        case .denied:
            self = .denied
        case .restricted:
            self = .restricted
        @unknown default:
            self = .unknown
        }
    }

    var canReadLibrary: Bool {
        self == .authorized || self == .limited
    }
}

protocol PhotoLibraryAuthorizationProviding: Sendable {
    func currentAccessState() -> PhotoLibraryAccessState
    func requestAccess() async -> PhotoLibraryAccessState
}

struct SystemPhotoLibraryAuthorizationProvider: PhotoLibraryAuthorizationProviding {
    func currentAccessState() -> PhotoLibraryAccessState {
        PhotoLibraryAccessState(
            PHPhotoLibrary.authorizationStatus(for: .readWrite)
        )
    }

    func requestAccess() async -> PhotoLibraryAccessState {
        await withCheckedContinuation { continuation in
            PHPhotoLibrary.requestAuthorization(for: .readWrite) { status in
                continuation.resume(returning: PhotoLibraryAccessState(status))
            }
        }
    }
}
