import SnapKit
import UIKit

final class SMBLocalDiscoveryViewController: UIViewController {
    private struct ServiceRow {
        let generation: Int
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
    private var isShowingDiscoveryLoading = false
    private var browser: NetServiceBrowser?
    private var browserGeneration = 0
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

        configureUI()
    }

    deinit {
        discoveryFinishWorkItem?.cancel()
        browser?.delegate = nil
        browser?.stop()
        rows.forEach {
            $0.service.delegate = nil
            $0.service.stop()
        }
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        startDiscovery()
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        stopDiscovery(clearRows: false)
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
        stopDiscovery(clearRows: true)
        browserGeneration &+= 1
        let generation = browserGeneration
        let activeBrowser = NetServiceBrowser()
        activeBrowser.delegate = self
        browser = activeBrowser
        browserErrorMessage = nil

        isBrowsing = true
        isShowingDiscoveryLoading = true
        loadingIndicatorView.startAnimating()
        navigationItem.rightBarButtonItem = loadingBarButtonItem
        tableView.reloadData()
        activeBrowser.searchForServices(ofType: "_smb._tcp.", inDomain: "local.")

        let workItem = DispatchWorkItem { [weak self, weak activeBrowser] in
            guard let self, let activeBrowser else { return }
            self.finishDiscoveryIfNeeded(browser: activeBrowser, generation: generation)
        }
        discoveryFinishWorkItem = workItem
        DispatchQueue.main.asyncAfter(deadline: .now() + 3, execute: workItem)
    }

    private func finishDiscoveryIfNeeded(browser activeBrowser: NetServiceBrowser, generation: Int) {
        guard browser === activeBrowser,
              browserGeneration == generation else { return }
        isShowingDiscoveryLoading = false
        discoveryFinishWorkItem?.cancel()
        discoveryFinishWorkItem = nil
        loadingIndicatorView.stopAnimating()
        navigationItem.rightBarButtonItem = refreshBarButtonItem
        tableView.refreshControl?.endRefreshing()
        tableView.reloadData()
    }

    private func stopDiscovery(clearRows: Bool) {
        browserGeneration &+= 1
        discoveryFinishWorkItem?.cancel()
        discoveryFinishWorkItem = nil
        browser?.delegate = nil
        browser?.stop()
        browser = nil
        rows.forEach {
            $0.service.delegate = nil
            $0.service.stop()
        }
        if clearRows {
            rows.removeAll()
            browserErrorMessage = nil
            tableView.reloadData()
        }
        isBrowsing = false
        isShowingDiscoveryLoading = false
        loadingIndicatorView.stopAnimating()
        navigationItem.rightBarButtonItem = refreshBarButtonItem
        tableView.refreshControl?.endRefreshing()
    }

    private func updateRow(for service: NetService, mutate: (inout ServiceRow) -> Void) {
        guard let index = rows.firstIndex(where: {
            $0.service === service && $0.generation == browserGeneration
        }) else { return }
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
        stopDiscovery(clearRows: false)
        navigationController?.pushViewController(loginVC, animated: true)
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }
}

extension SMBLocalDiscoveryViewController: NetServiceBrowserDelegate, NetServiceDelegate {
    func netServiceBrowser(_ activeBrowser: NetServiceBrowser, didFind service: NetService, moreComing: Bool) {
        guard browser === activeBrowser, isBrowsing else { return }
        guard !rows.contains(where: { $0.service === service }) else { return }
        service.delegate = self
        rows.append(ServiceRow(
            generation: browserGeneration,
            service: service,
            resolvedHost: nil,
            port: nil,
            resolveError: nil
        ))
        service.resolve(withTimeout: 5)

        if !moreComing {
            rows.sort { $0.displayName.localizedCaseInsensitiveCompare($1.displayName) == .orderedAscending }
            tableView.reloadData()
        }
    }

    func netServiceBrowser(_ activeBrowser: NetServiceBrowser, didRemove service: NetService, moreComing: Bool) {
        guard browser === activeBrowser, isBrowsing else { return }
        service.delegate = nil
        service.stop()
        rows.removeAll { $0.service === service }
        if !moreComing {
            tableView.reloadData()
        }
    }

    func netServiceBrowserDidStopSearch(_ activeBrowser: NetServiceBrowser) {
        guard browser === activeBrowser else { return }
        isBrowsing = false
        finishDiscoveryIfNeeded(browser: activeBrowser, generation: browserGeneration)
    }

    func netServiceBrowser(_ activeBrowser: NetServiceBrowser, didNotSearch errorDict: [String: NSNumber]) {
        guard browser === activeBrowser, isBrowsing else { return }
        browserErrorMessage = String(localized: "auth.smb.discovery.failedToDiscover")
        isBrowsing = false
        finishDiscoveryIfNeeded(browser: activeBrowser, generation: browserGeneration)
        activeBrowser.delegate = nil
        activeBrowser.stop()
        browser = nil
        tableView.reloadData()
    }

    func netServiceDidResolveAddress(_ sender: NetService) {
        let resolvedHost = sender.hostName?.trimmingCharacters(in: CharacterSet(charactersIn: "."))
        updateRow(for: sender) { row in
            row.resolvedHost = resolvedHost
            row.port = SMBEndpoint.effectivePort(sender.port)
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
            if isShowingDiscoveryLoading {
                content.text = String(localized: "auth.smb.discovery.searching")
                content.secondaryText = nil
            } else {
                content.text = browserErrorMessage == nil ? String(localized: "auth.smb.discovery.noServices") : String(localized: "auth.smb.discovery.discoveryFailed")
                content.secondaryText = browserErrorMessage == nil ? String(localized: "auth.smb.discovery.noServicesHint") : browserErrorMessage
            }
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
