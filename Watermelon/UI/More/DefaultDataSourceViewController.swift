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
    private let readSource: () -> LocalDataSource?
    private let readAlbums: () -> [LocalAlbumReference]
    private let saveSource: (LocalDataSource?) throws -> Void
    private let inheritedSource: (() -> LocalDataSource)?
    private let footer: String
    private var lastPresentedError: LocalDataSourceError?

    convenience init(service: PhotoLibraryService, sourceStore: LocalDataSourceStore = .shared) {
        self.init(
            service: service,
            title: String(localized: "settings.defaultDeviceScope.title"),
            footer: String(localized: "settings.defaultDeviceScope.footer"),
            readSource: { sourceStore.defaultSource },
            readAlbums: { sourceStore.albumReferences },
            saveSource: { source in
                guard let source else { return }
                try sourceStore.setDefault(source)
                NotificationCenter.default.post(name: .SettingsUpdate, object: nil)
            }
        )
    }

    convenience init(dependencies: DependencyContainer, profile: ServerProfileRecord) {
        var currentProfile = profile
        var footer = String(localized: "dataSource.node.footer")
        if profile.resolvedStorageType != .externalVolume {
            footer += "\n\n" + String(localized: "backgroundBackup.dataSource.footer")
        }
        self.init(
            service: dependencies.photoLibraryService,
            title: String(localized: "settings.defaultDeviceScope.title"),
            footer: footer,
            inheritedSource: { LocalDataSourceStore.shared.defaultSource },
            readSource: { currentProfile.backupDataSourceOverride },
            readAlbums: { currentProfile.defaultBackupDataSource().albums },
            saveSource: { source in
                guard let id = currentProfile.id else { throw RemoteStorageClientError.invalidConfiguration }
                currentProfile = try dependencies.saveNodeBackupDataSource(source, profileID: id)
            }
        )
    }

    init(
        service: PhotoLibraryService,
        title: String,
        footer: String,
        inheritedSource: (() -> LocalDataSource)? = nil,
        readSource: @escaping () -> LocalDataSource?,
        readAlbums: @escaping () -> [LocalAlbumReference],
        saveSource: @escaping (LocalDataSource?) throws -> Void
    ) {
        self.service = service
        self.readSource = readSource
        self.readAlbums = readAlbums
        self.saveSource = saveSource
        self.inheritedSource = inheritedSource
        self.footer = footer
        super.init(style: .insetGrouped)
        self.title = title
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        tableView.backgroundColor = .appBackground
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        guard let source = readSource() ?? inheritedSource?(), source.kind == .albums else { return }
        do {
            try service.validateAlbumSelection(
                source.scope.selectedAlbumIdentifiers,
                names: Dictionary(source.albums.map { ($0.id, $0.name) }, uniquingKeysWith: { _, new in new })
            )
            lastPresentedError = nil
        } catch let error as LocalDataSourceError {
            guard error != lastPresentedError else { return }
            lastPresentedError = error
            LocalAlbumSelectionPresentation.showError(error, from: self) { [weak self] in self?.selectAlbums() }
        } catch {}
    }

    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        LocalDataSource.Kind.allCases.count + (inheritedSource == nil ? 0 : 1)
    }

    override func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        footer
    }

    override func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        if let inheritedSource, indexPath.row == 0 {
            let cell = UITableViewCell(style: .subtitle, reuseIdentifier: nil)
            cell.textLabel?.text = String(localized: "dataSource.useAppDefault")
            cell.detailTextLabel?.text = inheritedSource().title
            cell.detailTextLabel?.numberOfLines = 0
            cell.accessoryType = readSource() == nil ? .checkmark : .none
            return cell
        }
        let kind = LocalDataSource.Kind.allCases[indexPath.row - (inheritedSource == nil ? 0 : 1)]
        let source = LocalDataSource(kind: kind, albums: kind == .albums ? readAlbums() : [])
        let cell = UITableViewCell(style: .subtitle, reuseIdentifier: nil)
        cell.textLabel?.text = kind == .albums ? String(localized: "home.localSource.specificAlbums") : source.title
        cell.detailTextLabel?.text = source.subtitle
        cell.detailTextLabel?.numberOfLines = 0
        cell.accessoryType = readSource()?.kind == kind ? .checkmark : .none
        return cell
    }

    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        if inheritedSource != nil, indexPath.row == 0 {
            save(nil)
            return
        }
        let kind = LocalDataSource.Kind.allCases[indexPath.row - (inheritedSource == nil ? 0 : 1)]
        if kind == .albums { selectAlbums() } else { save(LocalDataSource(kind: kind)) }
    }

    private func selectAlbums() {
        LocalAlbumSelectionPresentation.showPicker(
            from: self, service: service, selectedIDs: Set(readAlbums().map(\.id))
        ) { [weak self] albums in
            self?.save(LocalDataSource(kind: .albums, albums: albums.map(LocalAlbumReference.init)))
        }
    }

    private func save(_ source: LocalDataSource?) {
        do {
            try saveSource(source)
            lastPresentedError = nil
            tableView.reloadData()
        } catch let error as LocalDataSourceError {
            LocalAlbumSelectionPresentation.showError(error, from: self) { [weak self] in self?.selectAlbums() }
        } catch {
            let alert = UIAlertController(
                title: String(localized: "common.error"),
                message: UserFacingErrorLocalizer.message(for: error),
                preferredStyle: .alert
            )
            alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
            present(alert, animated: ConsideringUser.animated)
        }
    }
}
