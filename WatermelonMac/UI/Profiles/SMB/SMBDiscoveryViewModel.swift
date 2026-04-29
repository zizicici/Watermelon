import Combine
import Foundation

@MainActor
final class SMBDiscoveryViewModel: NSObject, ObservableObject {
    struct ServiceRow: Identifiable {
        let id: String
        let name: String
        var host: String?
        var port: Int?
        var error: String?

        var isReady: Bool { host != nil && port != nil }
    }

    @Published private(set) var rows: [ServiceRow] = []
    @Published private(set) var isBrowsing = false
    @Published private(set) var browserError: String?

    private let browser = NetServiceBrowser()
    private var pendingServices: [String: NetService] = [:]
    private var finishWorkItem: DispatchWorkItem?

    override init() {
        super.init()
        browser.delegate = self
    }

    deinit {
        finishWorkItem?.cancel()
        browser.stop()
        for service in pendingServices.values {
            service.stop()
        }
    }

    func startDiscovery() {
        finishWorkItem?.cancel()
        for service in pendingServices.values { service.stop() }
        pendingServices.removeAll()
        rows.removeAll()
        browserError = nil

        browser.stop()
        isBrowsing = true
        browser.searchForServices(ofType: "_smb._tcp.", inDomain: "local.")

        let work = DispatchWorkItem { [weak self] in
            self?.finishDiscovery()
        }
        finishWorkItem = work
        DispatchQueue.main.asyncAfter(deadline: .now() + 3, execute: work)
    }

    private func finishDiscovery() {
        guard isBrowsing else { return }
        isBrowsing = false
        finishWorkItem?.cancel()
        finishWorkItem = nil
    }

    private func rowID(for service: NetService) -> String {
        "\(service.domain)|\(service.type)|\(service.name)"
    }

    fileprivate func updateRow(for service: NetService, mutate: (inout ServiceRow) -> Void) {
        let id = rowID(for: service)
        guard let index = rows.firstIndex(where: { $0.id == id }) else { return }
        mutate(&rows[index])
    }
}

extension SMBDiscoveryViewModel: NetServiceBrowserDelegate, NetServiceDelegate {
    nonisolated func netServiceBrowser(_ browser: NetServiceBrowser, didFind service: NetService, moreComing: Bool) {
        Task { @MainActor in
            service.delegate = self
            let id = self.rowID(for: service)
            if !self.rows.contains(where: { $0.id == id }) {
                self.rows.append(ServiceRow(id: id, name: service.name))
                self.pendingServices[id] = service
                service.resolve(withTimeout: 5)
            }
            if !moreComing {
                self.finishDiscovery()
                self.rows.sort { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
            }
        }
    }

    nonisolated func netServiceBrowser(_ browser: NetServiceBrowser, didRemove service: NetService, moreComing: Bool) {
        Task { @MainActor in
            let id = self.rowID(for: service)
            self.rows.removeAll { $0.id == id }
            self.pendingServices.removeValue(forKey: id)
        }
    }

    nonisolated func netServiceBrowser(_ browser: NetServiceBrowser, didNotSearch errorDict: [String: NSNumber]) {
        Task { @MainActor in
            self.finishDiscovery()
            self.browserError = String(localized: "smb.discovery.failed")
        }
    }

    nonisolated func netServiceDidResolveAddress(_ sender: NetService) {
        let resolvedHost = sender.hostName?.trimmingCharacters(in: CharacterSet(charactersIn: "."))
        let port = sender.port
        Task { @MainActor in
            self.updateRow(for: sender) { row in
                row.host = resolvedHost
                row.port = port
                row.error = nil
            }
        }
    }

    nonisolated func netService(_ sender: NetService, didNotResolve errorDict: [String: NSNumber]) {
        Task { @MainActor in
            self.updateRow(for: sender) { row in
                row.error = String(localized: "smb.discovery.resolveFailed")
            }
        }
    }
}
