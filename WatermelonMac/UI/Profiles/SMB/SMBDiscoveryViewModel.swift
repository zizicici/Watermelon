@preconcurrency import Foundation

@MainActor
final class SMBDiscoveryViewModel: NSObject {
    nonisolated private struct BrowserReference: @unchecked Sendable {
        let value: NetServiceBrowser
    }

    nonisolated private struct ServiceReference: @unchecked Sendable {
        let value: NetService
    }

    nonisolated private struct WorkItemReference: @unchecked Sendable {
        let value: DispatchWorkItem
    }

    struct ServiceRow: @unchecked Sendable {
        let id: String
        let generation: Int
        let service: NetService
        var host: String?
        var port: Int?
        var error: String?

        var name: String { service.name }
        var isReady: Bool { host != nil && port != nil }
    }

    var onChange: (() -> Void)?

    private(set) var rows: [ServiceRow] = []
    private(set) var isShowingLoading = false
    private(set) var browserError: String?

    private var browser: BrowserReference?
    private var browserGeneration = 0
    private var finishWorkItem: WorkItemReference?

    deinit {
        finishWorkItem?.value.cancel()
        browser?.value.delegate = nil
        browser?.value.stop()
        rows.forEach {
            $0.service.delegate = nil
            $0.service.stop()
        }
    }

    func startDiscovery() {
        stopDiscovery(clearRows: true)
        browserGeneration &+= 1
        let generation = browserGeneration
        let activeBrowser = NetServiceBrowser()
        let browserReference = BrowserReference(value: activeBrowser)
        activeBrowser.delegate = self
        browser = browserReference
        browserError = nil
        isShowingLoading = true
        notifyChange()
        activeBrowser.searchForServices(
            ofType: "_smb._tcp.",
            inDomain: "local."
        )

        let workItem = DispatchWorkItem {
            [weak self, browserReference] in
            guard let self else { return }
            self.finishDiscovery(
                browser: browserReference,
                generation: generation
            )
        }
        finishWorkItem = WorkItemReference(value: workItem)
        DispatchQueue.main.asyncAfter(
            deadline: .now() + 3,
            execute: workItem
        )
    }

    func stopDiscovery(clearRows: Bool) {
        browserGeneration &+= 1
        finishWorkItem?.value.cancel()
        finishWorkItem = nil
        browser?.value.delegate = nil
        browser?.value.stop()
        browser = nil
        rows.forEach {
            $0.service.delegate = nil
            $0.service.stop()
        }
        if clearRows {
            rows.removeAll()
            browserError = nil
        }
        isShowingLoading = false
        notifyChange()
    }

    private func finishDiscovery(
        browser activeBrowser: BrowserReference,
        generation: Int
    ) {
        guard browser?.value === activeBrowser.value,
              browserGeneration == generation else {
            return
        }
        isShowingLoading = false
        finishWorkItem?.value.cancel()
        finishWorkItem = nil
        notifyChange()
    }

    private func rowID(for service: NetService) -> String {
        "\(service.domain)|\(service.type)|\(service.name)"
    }

    private func updateRow(
        for service: NetService,
        mutate: (inout ServiceRow) -> Void
    ) {
        guard let index = rows.firstIndex(where: {
            $0.service === service
                && $0.generation == browserGeneration
        }) else {
            return
        }
        mutate(&rows[index])
        notifyChange()
    }

    private func notifyChange() {
        onChange?()
    }
}

extension SMBDiscoveryViewModel:
    NetServiceBrowserDelegate,
    NetServiceDelegate
{
    nonisolated func netServiceBrowser(
        _ activeBrowser: NetServiceBrowser,
        didFind service: NetService,
        moreComing: Bool
    ) {
        let browserReference = BrowserReference(value: activeBrowser)
        let serviceReference = ServiceReference(value: service)
        MainActor.assumeIsolated {
            let activeBrowser = browserReference.value
            let service = serviceReference.value
            guard browser?.value === activeBrowser else { return }
            guard !rows.contains(where: { $0.service === service }) else {
                return
            }
            service.delegate = self
            rows.append(
                ServiceRow(
                    id: rowID(for: service),
                    generation: browserGeneration,
                    service: service
                )
            )
            service.resolve(withTimeout: 5)
            if !moreComing {
                rows.sort {
                    $0.name.localizedCaseInsensitiveCompare($1.name)
                        == .orderedAscending
                }
                notifyChange()
            }
        }
    }

    nonisolated func netServiceBrowser(
        _ activeBrowser: NetServiceBrowser,
        didRemove service: NetService,
        moreComing: Bool
    ) {
        let browserReference = BrowserReference(value: activeBrowser)
        let serviceReference = ServiceReference(value: service)
        MainActor.assumeIsolated {
            let activeBrowser = browserReference.value
            let service = serviceReference.value
            guard browser?.value === activeBrowser else { return }
            service.delegate = nil
            service.stop()
            rows.removeAll { $0.service === service }
            if !moreComing {
                notifyChange()
            }
        }
    }

    nonisolated func netServiceBrowserDidStopSearch(
        _ activeBrowser: NetServiceBrowser
    ) {
        let browserReference = BrowserReference(value: activeBrowser)
        MainActor.assumeIsolated {
            guard browser?.value === browserReference.value else {
                return
            }
            finishDiscovery(
                browser: browserReference,
                generation: browserGeneration
            )
        }
    }

    nonisolated func netServiceBrowser(
        _ activeBrowser: NetServiceBrowser,
        didNotSearch errorDict: [String: NSNumber]
    ) {
        let browserReference = BrowserReference(value: activeBrowser)
        MainActor.assumeIsolated {
            let activeBrowser = browserReference.value
            guard browser?.value === activeBrowser else { return }
            browserError = String(
                localized: "auth.smb.discovery.failedToDiscover"
            )
            finishDiscovery(
                browser: browserReference,
                generation: browserGeneration
            )
            activeBrowser.delegate = nil
            activeBrowser.stop()
            browser = nil
            notifyChange()
        }
    }

    nonisolated func netServiceDidResolveAddress(_ sender: NetService) {
        let serviceReference = ServiceReference(value: sender)
        let host = sender.hostName?.trimmingCharacters(
            in: CharacterSet(charactersIn: ".")
        )
        let port = SMBEndpoint.effectivePort(sender.port)
        MainActor.assumeIsolated {
            updateRow(for: serviceReference.value) { row in
                row.host = host
                row.port = port
                row.error = nil
            }
        }
    }

    nonisolated func netService(
        _ sender: NetService,
        didNotResolve errorDict: [String: NSNumber]
    ) {
        let serviceReference = ServiceReference(value: sender)
        MainActor.assumeIsolated {
            updateRow(for: serviceReference.value) { row in
                row.error = String(
                    localized: "auth.smb.discovery.resolveFailed"
                )
            }
        }
    }
}
