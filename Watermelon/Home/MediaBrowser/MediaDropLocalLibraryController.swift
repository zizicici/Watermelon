import MoreKit
import Photos
import UIKit

@MainActor
final class MediaDropLocalLibraryController {
    private let photoLibraryService: PhotoLibraryService
    private let makeAlbumBrowser: (LocalAlbumDescriptor) -> UIViewController?
    private(set) var scope: HomeLocalLibraryScope
    private var selectedAlbums: [LocalAlbumDescriptor] = []

    var canChangeScope: () -> Bool = { true }
    var onScopeChanged: (() -> Void)?
    var onTitleChanged: (() -> Void)?

    init(
        photoLibraryService: PhotoLibraryService,
        initialMediaFilter: PhotoLibraryMediaFilter = DefaultDeviceMediaScopeSetting.getValue().mediaFilter,
        makeAlbumBrowser: @escaping (LocalAlbumDescriptor) -> UIViewController?
    ) {
        self.photoLibraryService = photoLibraryService
        scope = .device(initialMediaFilter)
        self.makeAlbumBrowser = makeAlbumBrowser
    }

    var title: String {
        switch scope {
        case .device(let filter):
            return HomeLocalLibraryMenu.deviceTitle(for: filter, isPad: UIDevice.current.userInterfaceIdiom == .pad)
        case .albums(let identifiers):
            if identifiers.count == 1, let album = selectedAlbums.first {
                return album.title
            }
            return String.localizedStringWithFormat(
                String(localized: "home.localSource.albumCount"),
                identifiers.count
            )
        }
    }

    func makeSource() -> MediaBrowserSource {
        TransferLocalMediaSource(
            photoLibraryService: photoLibraryService,
            query: scope.photoLibraryQuery
        )
    }

    func makeMenu(presenter: UIViewController) -> UIMenu {
        let attributes: UIMenuElement.Attributes = canChangeScope() ? [] : .disabled
        let deviceMenu = HomeLocalLibraryMenu.deviceMenu(
            scope: scope,
            isPad: presenter.traitCollection.userInterfaceIdiom == .pad,
            attributes: attributes
        ) { [weak self] scope in
            self?.setScope(scope)
        }
        let specificAlbums = UIAction(
            title: String(localized: "home.localSource.specificAlbums"),
            image: UIImage(systemName: "photo.stack"),
            attributes: attributes,
            state: scope.isSpecificAlbums ? .on : .off
        ) { [weak self, weak presenter] _ in
            guard let self, let presenter, self.canChangeScope() else { return }
            self.openAlbumPicker(from: presenter)
        }
        return UIMenu(children: [deviceMenu, specificAlbums])
    }

    func setScope(_ scope: HomeLocalLibraryScope, albums: [LocalAlbumDescriptor] = []) {
        guard canChangeScope() else { return }
        let didChangeScope = scope != self.scope
        let previousTitle = title
        self.scope = scope
        if didChangeScope || !albums.isEmpty {
            selectedAlbums = albums
        }
        if didChangeScope {
            onScopeChanged?()
        } else if title != previousTitle {
            onTitleChanged?()
        }
    }

    private func openAlbumPicker(from presenter: UIViewController) {
        let access = LocalPhotoAccessState(authorizationStatus: photoLibraryService.authorizationStatus())
        guard access.isAuthorized else {
            if photoLibraryService.authorizationStatus() == .notDetermined {
                PHPhotoLibrary.requestAuthorization(for: .readWrite) { [weak self, weak presenter] status in
                    Task { @MainActor in
                        guard let self, let presenter,
                              LocalPhotoAccessState(authorizationStatus: status).isAuthorized,
                              self.canChangeScope() else { return }
                        self.openAlbumPicker(from: presenter)
                    }
                }
            } else if let url = URL(string: UIApplication.openSettingsURLString) {
                UIApplication.shared.open(url)
            }
            return
        }

        let picker = LocalAlbumPickerViewController(
            photoLibraryService: photoLibraryService,
            selectedAlbumIDs: scope.selectedAlbumIdentifiers,
            makeAlbumBrowser: makeAlbumBrowser
        ) { [weak self] albums in
            self?.setScope(.albums(Set(albums.map(\.localIdentifier))), albums: albums)
        }
        let container = UINavigationController(rootViewController: picker)
        if let sheet = container.sheetPresentationController {
            sheet.detents = [.medium(), .large()]
            sheet.prefersGrabberVisible = true
        }
        presenter.present(container, animated: ConsideringUser.animated)
    }
}
