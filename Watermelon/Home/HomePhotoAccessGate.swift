import Foundation
@preconcurrency import Photos

@MainActor
final class HomePhotoAccessGate {
    private(set) var state: LocalPhotoAccessState
    private let photoLibraryService: PhotoLibraryService

    init(photoLibraryService: PhotoLibraryService) {
        self.photoLibraryService = photoLibraryService
        self.state = LocalPhotoAccessState(authorizationStatus: photoLibraryService.authorizationStatus())
    }

    /// Refresh the cached state from PhotoKit. Returns `true` if it changed.
    @discardableResult
    func refresh() -> Bool {
        let live = LocalPhotoAccessState(authorizationStatus: photoLibraryService.authorizationStatus())
        guard live != state else { return false }
        state = live
        return true
    }

    /// Whether the live system state diverges from the cached state — used by the
    /// foreground-resume gate to decide whether to schedule a reload without committing
    /// the new value (the refresh loop will commit it later via `refresh()`).
    func hasSystemStateDiverged() -> Bool {
        LocalPhotoAccessState(authorizationStatus: photoLibraryService.authorizationStatus()) != state
    }

    func currentSystemAuthorizationStatus() -> PHAuthorizationStatus {
        photoLibraryService.authorizationStatus()
    }

    func requestAuthorization() async -> PHAuthorizationStatus {
        await photoLibraryService.requestAuthorization()
    }
}
