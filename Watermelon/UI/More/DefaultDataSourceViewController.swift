import MoreKit
import Photos
import UIKit

@MainActor
enum LocalAlbumSelectionPresentation {
    static func showError(_ error: LocalDataSourceError, from presenter: UIViewController, reselect: @escaping () -> Void) {
        guard presenter.presentedViewController == nil else { return }
        let alert = UIAlertController(
            title: String(localized: "dataSource.error.title"),
            message: error.localizedDescription,
            preferredStyle: .alert
        )
        let needsPermission = error == .fullPhotoAccessRequired
        alert.addAction(UIAlertAction(
            title: needsPermission ? String(localized: "dataSource.openSettings") : String(localized: "dataSource.reselect"),
            style: .default
        ) { [weak alert] _ in
            if needsPermission {
                if let url = URL(string: UIApplication.openSettingsURLString) { UIApplication.shared.open(url) }
            } else {
                alert?.dismiss(animated: ConsideringUser.animated, completion: reselect)
            }
        })
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        presenter.present(alert, animated: ConsideringUser.animated)
    }

    static func showPicker(
        from presenter: UIViewController,
        service: PhotoLibraryService,
        selectedIDs: Set<String>,
        makeAlbumBrowser: @escaping (LocalAlbumDescriptor) -> UIViewController? = { _ in nil },
        onDone: @escaping ([LocalAlbumDescriptor]) -> Void
    ) {
        if service.authorizationStatus() == .notDetermined {
            PHPhotoLibrary.requestAuthorization(for: .readWrite) { [weak presenter] _ in
                Task { @MainActor [weak presenter] in
                    guard let presenter else { return }
                    showPicker(from: presenter, service: service, selectedIDs: selectedIDs, makeAlbumBrowser: makeAlbumBrowser, onDone: onDone)
                }
            }
            return
        }
        guard service.authorizationStatus() == .authorized else {
            showError(.fullPhotoAccessRequired, from: presenter, reselect: {})
            return
        }
        let picker = LocalAlbumPickerViewController(
            photoLibraryService: service,
            selectedAlbumIDs: selectedIDs,
            makeAlbumBrowser: makeAlbumBrowser,
            onDone: onDone
        )
        let container = UINavigationController(rootViewController: picker)
        container.sheetPresentationController?.prefersGrabberVisible = true
        container.sheetPresentationController?.detents = [.medium(), .large()]
        presenter.present(container, animated: ConsideringUser.animated)
    }
}

final class DefaultDataSourceViewController: UITableViewController {
    private let service: PhotoLibraryService
    private let sourceStore: LocalDataSourceStore
    private var lastPresentedError: LocalDataSourceError?

    init(service: PhotoLibraryService, sourceStore: LocalDataSourceStore = .shared) {
        self.service = service
        self.sourceStore = sourceStore
        super.init(style: .insetGrouped)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "settings.defaultDeviceScope.title")
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        let source = sourceStore.defaultSource
        guard source.kind == .albums else { return }
        do {
            try service.validateAlbumSelection(source.scope.selectedAlbumIdentifiers)
            lastPresentedError = nil
        } catch let error as LocalDataSourceError {
            guard error != lastPresentedError else { return }
            lastPresentedError = error
            LocalAlbumSelectionPresentation.showError(error, from: self) { [weak self] in self?.selectAlbums() }
        } catch {}
    }

    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        LocalDataSource.Kind.allCases.count
    }

    override func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        String(localized: "settings.defaultDeviceScope.footer")
    }

    override func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let kind = LocalDataSource.Kind.allCases[indexPath.row]
        let source = sourceStore.source(for: kind)
        let cell = UITableViewCell(style: .subtitle, reuseIdentifier: nil)
        cell.textLabel?.text = kind == .albums ? String(localized: "home.localSource.specificAlbums") : source.title
        cell.detailTextLabel?.text = source.subtitle
        cell.detailTextLabel?.numberOfLines = 0
        cell.accessoryType = sourceStore.defaultSource.kind == kind ? .checkmark : .none
        return cell
    }

    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        let kind = LocalDataSource.Kind.allCases[indexPath.row]
        if kind == .albums { selectAlbums() } else { save(sourceStore.source(for: kind)) }
    }

    private func selectAlbums() {
        LocalAlbumSelectionPresentation.showPicker(
            from: self, service: service, selectedIDs: Set(sourceStore.albumReferences.map(\.id))
        ) { [weak self] albums in
            self?.save(LocalDataSource(kind: .albums, albums: albums.map(LocalAlbumReference.init)))
        }
    }

    private func save(_ source: LocalDataSource) {
        do {
            try sourceStore.setDefault(source)
            lastPresentedError = nil
            tableView.reloadData()
            NotificationCenter.default.post(name: .SettingsUpdate, object: nil)
        } catch let error as LocalDataSourceError {
            LocalAlbumSelectionPresentation.showError(error, from: self) { [weak self] in self?.selectAlbums() }
        } catch {
            assertionFailure(error.localizedDescription)
        }
    }
}
