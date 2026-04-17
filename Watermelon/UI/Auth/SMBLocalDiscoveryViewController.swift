import SnapKit
import UIKit

final class SMBLocalDiscoveryViewController: UIViewController {
    private struct ServiceRow {
        let id: String
        let service: NetService
        var resolvedHost: String?
        var port: Int?
        var resolveError: String?

        var displayName: String {
            service.name
        }
    }

    private let dependencies: DependencyContainer
    private let shouldPopToRootOnSave: Bool
    private let onSaved: (ServerProfileRecord, String) -> Void

    private let browser = NetServiceBrowser()
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private lazy var refreshBarButtonItem = UIBarButtonItem(
        barButtonSystemItem: .refresh,
        target: self,
        action: #selector(refreshDiscovery)
    )
    private lazy var loadingIndicatorView = UIActivityIndicatorView(style: .medium)
    private lazy var loadingBarButtonItem = UIBarButtonItem(customView: loadingIndicatorView)

    private var rows: [ServiceRow] = []
    private var isBrowsing = false
    private var browserErrorMessage: String?
    private var discoveryFinishWorkItem: DispatchWorkItem?

    init(
        dependencies: DependencyContainer,
        shouldPopToRootOnSave: Bool = true,
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) {
        self.dependencies = dependencies
        self.shouldPopToRootOnSave = shouldPopToRootOnSave
        self.onSaved = onSaved
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "auth.smb.discovery.title")

        browser.delegate = self

        configureUI()
        startDiscovery()
    }

    deinit {
        discoveryFinishWorkItem?.cancel()
        browser.stop()
        rows.forEach { $0.service.stop() }
    }

    private func configureUI() {
        navigationItem.rightBarButtonItem = refreshBarButtonItem

        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.rowHeight = UITableView.automaticDimension
        tableView.estimatedRowHeight = 44
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "ServiceCell")

        let refreshControl = UIRefreshControl()
        refreshControl.addTarget(self, action: #selector(refreshDiscovery), for: .valueChanged)
        tableView.refreshControl = refreshControl

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    @objc
    private func refreshDiscovery() {
        startDiscovery()
    }

    private func startDiscovery() {
        discoveryFinishWorkItem?.cancel()
        rows.forEach { $0.service.stop() }
        rows.removeAll()
        browserErrorMessage = nil
        tableView.reloadData()

        browser.stop()
        isBrowsing = true
        loadingIndicatorView.startAnimating()
        navigationItem.rightBarButtonItem = loadingBarButtonItem
        browser.searchForServices(ofType: "_smb._tcp.", inDomain: "local.")

        let workItem = DispatchWorkItem { [weak self] in
            self?.finishDiscoveryIfNeeded()
        }
        discoveryFinishWorkItem = workItem
        DispatchQueue.main.asyncAfter(deadline: .now() + 3, execute: workItem)
    }

    private func finishDiscoveryIfNeeded() {
        guard isBrowsing else { return }
        isBrowsing = false
        discoveryFinishWorkItem?.cancel()
        discoveryFinishWorkItem = nil
        loadingIndicatorView.stopAnimating()
        navigationItem.rightBarButtonItem = refreshBarButtonItem
        tableView.refreshControl?.endRefreshing()
    }

    private func rowID(for service: NetService) -> String {
        "\(service.domain)|\(service.type)|\(service.name)"
    }

    private func updateRow(for service: NetService, mutate: (inout ServiceRow) -> Void) {
        let id = rowID(for: service)
        guard let index = rows.firstIndex(where: { $0.id == id }) else { return }
        mutate(&rows[index])
        tableView.reloadRows(at: [IndexPath(row: index, section: 0)], with: .none)
    }

    private func openDiscoveredService(_ row: ServiceRow) {
        guard let host = row.resolvedHost, let port = row.port else {
            presentAlert(title: String(localized: "auth.smb.discovery.notReady"), message: String(localized: "auth.smb.discovery.notReadyMessage"))
            return
        }

        let draft = SMBServerLoginDraft(
            name: row.displayName,
            host: host,
            port: port,
            username: "",
            domain: nil
        )
        let loginVC = AddSMBServerLoginViewController(
            dependencies: dependencies,
            draft: draft,
            shouldPopToRootOnSave: shouldPopToRootOnSave,
            onSaved: onSaved
        )
        navigationController?.pushViewController(loginVC, animated: true)
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }
}

extension SMBLocalDiscoveryViewController: NetServiceBrowserDelegate, NetServiceDelegate {
    func netServiceBrowser(_ browser: NetServiceBrowser, didFind service: NetService, moreComing: Bool) {
        service.delegate = self
        rows.append(ServiceRow(id: rowID(for: service), service: service, resolvedHost: nil, port: nil, resolveError: nil))
        service.resolve(withTimeout: 5)

        if !moreComing {
            finishDiscoveryIfNeeded()
            rows.sort { $0.displayName.localizedCaseInsensitiveCompare($1.displayName) == .orderedAscending }
            tableView.reloadData()
        }
    }

    func netServiceBrowser(_ browser: NetServiceBrowser, didRemove service: NetService, moreComing: Bool) {
        let id = rowID(for: service)
        rows.removeAll { $0.id == id }
        if !moreComing {
            tableView.reloadData()
        }
    }

    func netServiceBrowserDidStopSearch(_ browser: NetServiceBrowser) {
        finishDiscoveryIfNeeded()
    }

    func netServiceBrowser(_ browser: NetServiceBrowser, didNotSearch errorDict: [String: NSNumber]) {
        finishDiscoveryIfNeeded()
        browserErrorMessage = String(localized: "auth.smb.discovery.failedToDiscover")
        tableView.reloadData()
    }

    func netServiceDidResolveAddress(_ sender: NetService) {
        let resolvedHost = sender.hostName?.trimmingCharacters(in: CharacterSet(charactersIn: "."))
        updateRow(for: sender) { row in
            row.resolvedHost = resolvedHost
            row.port = sender.port
            row.resolveError = nil
        }
    }

    func netService(_ sender: NetService, didNotResolve errorDict: [String: NSNumber]) {
        updateRow(for: sender) { row in
            row.resolveError = String(localized: "auth.smb.discovery.resolveFailed")
        }
    }
}

extension SMBLocalDiscoveryViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        1
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        max(rows.count, 1)
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        String(localized: "auth.smb.discovery.sectionTitle")
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        browserErrorMessage ?? String(localized: "auth.smb.discovery.sectionFooter")
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "ServiceCell", for: indexPath)
        var content = UIListContentConfiguration.subtitleCell()

        if rows.isEmpty {
            content.text = browserErrorMessage == nil ? String(localized: "auth.smb.discovery.noServices") : String(localized: "auth.smb.discovery.discoveryFailed")
            content.secondaryText = browserErrorMessage == nil ? String(localized: "auth.smb.discovery.noServicesHint") : browserErrorMessage
            cell.selectionStyle = .none
            cell.accessoryType = .none
        } else {
            let row = rows[indexPath.row]
            content.text = row.displayName

            if let host = row.resolvedHost, let port = row.port {
                content.secondaryText = "\(host):\(port)"
                cell.accessoryType = .disclosureIndicator
            } else if let resolveError = row.resolveError {
                content.secondaryText = resolveError
                cell.accessoryType = .none
            } else {
                content.secondaryText = String(localized: "auth.smb.discovery.resolving")
                cell.accessoryType = .none
            }
            cell.selectionStyle = .default
        }

        cell.contentConfiguration = content
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard indexPath.row < rows.count else { return }
        openDiscoveredService(rows[indexPath.row])
    }
}
