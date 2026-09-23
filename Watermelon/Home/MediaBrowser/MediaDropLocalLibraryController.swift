import MoreKit
import Photos
import UIKit

@MainActor
final class MediaDropLocalLibraryController {
    private let photoLibraryService: PhotoLibraryService
    private let makeAlbumBrowser: (LocalAlbumDescriptor) -> UIViewController?
    private(set) var scope: HomeLocalLibraryScope
    private var selectedAlbums: [LocalAlbumDescriptor] = []
    private var defaultSource: LocalDataSource

    var canChangeScope: () -> Bool = { true }
    var onScopeChanged: (() -> Void)?
    var onTitleChanged: (() -> Void)?
    var onDataSourceError: ((LocalDataSourceError) -> Void)?

    init(
        photoLibraryService: PhotoLibraryService,
        initialMediaFilter: PhotoLibraryMediaFilter? = nil,
        makeAlbumBrowser: @escaping (LocalAlbumDescriptor) -> UIViewController?
    ) {
        self.photoLibraryService = photoLibraryService
        let source = LocalDataSourceStore.shared.defaultSource
        defaultSource = source
        scope = initialMediaFilter.map(HomeLocalLibraryScope.device) ?? source.scope
        selectedAlbums = LocalDataSourceStore.shared.albumReferences.map {
            LocalAlbumDescriptor(localIdentifier: $0.id, title: $0.name, assetCount: 0, thumbnailAssetIdentifier: nil)
        }
        self.makeAlbumBrowser = makeAlbumBrowser
    }

    func applyDefaultSourceIfNeeded() {
        guard canChangeScope() else { return }
        let source = LocalDataSourceStore.shared.defaultSource
        guard source != defaultSource else { return }
        defaultSource = source
        let albums = source.albums.map {
            LocalAlbumDescriptor(localIdentifier: $0.id, title: $0.name, assetCount: 0, thumbnailAssetIdentifier: nil)
        }
        setScope(source.scope, albums: albums)
    }

    var title: String {
        switch scope {
        case .device(let filter):
            return HomeLocalLibraryMenu.deviceTitle(for: filter, isPad: UIDevice.current.userInterfaceIdiom == .pad)
        case .albums(let identifiers):
            if identifiers.count == 1, let album = selectedAlbums.first(where: { identifiers.contains($0.localIdentifier) }) {
                return album.title
            }
            return String.localizedStringWithFormat(
                String(localized: "home.localSource.albumCount"),
                identifiers.count
            )
        }
    }

    func makeSource() -> MediaBrowserSource {
        let expectedScope = scope
        return TransferLocalMediaSource(
            photoLibraryService: photoLibraryService,
            query: scope.photoLibraryQuery,
            albumNames: Dictionary(selectedAlbums.map { ($0.localIdentifier, $0.title) }, uniquingKeysWith: { _, new in new }),
            onDataSourceError: { [weak self] error in
                guard let self, self.scope == expectedScope else { return }
                self.onDataSourceError?(error)
            }
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

    func validateSelection() throws {
        guard case .albums(let ids) = scope else { return }
        try photoLibraryService.validateAlbumSelection(
            ids, names: Dictionary(selectedAlbums.map { ($0.localIdentifier, $0.title) }, uniquingKeysWith: { _, new in new })
        )
    }

    func repairSelection(from presenter: UIViewController) {
        openAlbumPicker(from: presenter)
    }

    private func openAlbumPicker(from presenter: UIViewController) {
        let selectedIDs = scope.selectedAlbumIdentifiers
        let savedIDs = Set(LocalDataSourceStore.shared.albumReferences.map(\.id))
        let repairsSavedSelection = (try? validateSelection()) == nil
            && selectedIDs == savedIDs
        LocalAlbumSelectionPresentation.showPicker(
            from: presenter, service: photoLibraryService, selectedIDs: selectedIDs,
            makeAlbumBrowser: makeAlbumBrowser
        ) { [weak self] albums in
            if repairsSavedSelection {
                try? LocalDataSourceStore.shared.replaceAlbumSelection(albums.map(LocalAlbumReference.init))
                NotificationCenter.default.post(name: .SettingsUpdate, object: nil)
            }
            let scope: HomeLocalLibraryScope = albums.isEmpty ? .device(.all) : .albums(Set(albums.map(\.localIdentifier)))
            self?.setScope(scope, albums: albums)
        }
    }
}
