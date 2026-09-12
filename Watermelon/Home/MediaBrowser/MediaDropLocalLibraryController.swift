import Photos
import UIKit

@MainActor
final class MediaDropLocalLibraryController {
    private let photoLibraryService: PhotoLibraryService
    private let makeAlbumBrowser: (LocalAlbumDescriptor) -> UIViewController?
    private(set) var scope: HomeLocalLibraryScope = .allPhotos
    private var selectedAlbums: [LocalAlbumDescriptor] = []

    var canChangeScope: () -> Bool = { true }
    var onScopeChanged: (() -> Void)?
    var onTitleChanged: (() -> Void)?

    init(
        photoLibraryService: PhotoLibraryService,
        makeAlbumBrowser: @escaping (LocalAlbumDescriptor) -> UIViewController?
    ) {
        self.photoLibraryService = photoLibraryService
        self.makeAlbumBrowser = makeAlbumBrowser
    }

    var title: String {
        switch scope {
        case .allPhotos:
            return String(localized: "transfer.header.localLibrary")
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
        let allPhotos = UIAction(
            title: String(localized: "home.localSource.allPhotos"),
            image: UIImage(systemName: presenter.traitCollection.userInterfaceIdiom == .pad ? "ipad" : "iphone"),
            attributes: attributes,
            state: scope.isSpecificAlbums ? .off : .on
        ) { [weak self] _ in
            self?.setScope(.allPhotos)
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
        return UIMenu(children: [allPhotos, specificAlbums])
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
