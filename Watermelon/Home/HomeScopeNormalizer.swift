import Foundation

@MainActor
final class HomeScopeNormalizer {
    enum Alert {
        case albumsUnavailable
        case albumsUpdated
    }

    private static let alertDebounceInterval: CFAbsoluteTime = 2.0

    private let photoLibraryService: PhotoLibraryService
    private var lastAlertTime: CFAbsoluteTime = 0

    /// Surface a user-visible alert. Set by the store; the alert text is localized.
    var onAlert: ((String, String) -> Void)?

    init(photoLibraryService: PhotoLibraryService) {
        self.photoLibraryService = photoLibraryService
    }

    /// Pure normalize: returns the scope adjusted to current PhotoKit reality and the
    /// kind of alert that should be surfaced (if any). When auth is missing the input
    /// is returned unchanged — the caller's alerting flow handles the unauthorized
    /// branch separately.
    func normalize(_ scope: HomeLocalLibraryScope) -> (scope: HomeLocalLibraryScope, alert: Alert?) {
        guard case .albums(let ids) = scope else { return (scope, nil) }
        let access = LocalPhotoAccessState(authorizationStatus: photoLibraryService.authorizationStatus())
        guard access.isAuthorized else { return (scope, nil) }
        guard !ids.isEmpty else { return (.allPhotos, nil) }

        let existing = photoLibraryService.existingUserAlbumIdentifiers(in: ids)
        guard existing != ids else { return (scope, nil) }

        if existing.isEmpty {
            return (.allPhotos, .albumsUnavailable)
        }
        return (.albums(existing), .albumsUpdated)
    }

    /// Debounced alert emission. A flurry of normalize calls during a refresh storm
    /// surfaces one alert every `alertDebounceInterval`.
    func emitAlertIfNotDebounced(_ alert: Alert) {
        let now = CFAbsoluteTimeGetCurrent()
        guard now - lastAlertTime >= Self.alertDebounceInterval else { return }
        lastAlertTime = now
        switch alert {
        case .albumsUnavailable:
            onAlert?(
                String(localized: "home.alert.localAlbumsUnavailable"),
                String(localized: "home.alert.localAlbumsUnavailableMessage")
            )
        case .albumsUpdated:
            onAlert?(
                String(localized: "home.alert.localAlbumsUpdated"),
                String(localized: "home.alert.localAlbumsUpdatedMessage")
            )
        }
    }
}
