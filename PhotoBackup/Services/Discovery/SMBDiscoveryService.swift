import Foundation

struct DiscoveredSMBServer: Hashable {
    let serviceName: String
    let host: String
    let port: Int
}

final class SMBDiscoveryService: NSObject {
    private let browser = NetServiceBrowser()
    private var services: [NetService] = []
    private var resolved: [ObjectIdentifier: DiscoveredSMBServer] = [:]

    var onUpdate: (([DiscoveredSMBServer]) -> Void)?

    override init() {
        super.init()
        browser.delegate = self
    }

    func start() {
        stop()
        browser.searchForServices(ofType: "_smb._tcp.", inDomain: "local.")
    }

    func stop() {
        browser.stop()
        services.forEach { $0.stop() }
        services.removeAll()
        resolved.removeAll()
        onUpdate?([])
    }

    private func publish() {
        let values = resolved.values.sorted { lhs, rhs in
            if lhs.host == rhs.host {
                return lhs.serviceName < rhs.serviceName
            }
            return lhs.host < rhs.host
        }
        onUpdate?(values)
    }
}

extension SMBDiscoveryService: NetServiceBrowserDelegate, NetServiceDelegate {
    func netServiceBrowser(_ browser: NetServiceBrowser, didFind service: NetService, moreComing: Bool) {
        service.delegate = self
        services.append(service)
        service.resolve(withTimeout: 5)

        if !moreComing {
            publish()
        }
    }

    func netServiceBrowser(_ browser: NetServiceBrowser, didRemove service: NetService, moreComing: Bool) {
        services.removeAll(where: { $0 == service })
        resolved.removeValue(forKey: ObjectIdentifier(service))

        if !moreComing {
            publish()
        }
    }

    func netServiceDidResolveAddress(_ sender: NetService) {
        let host = sender.hostName?.trimmingCharacters(in: CharacterSet(charactersIn: ".")) ?? sender.name
        let entry = DiscoveredSMBServer(serviceName: sender.name, host: host, port: sender.port)
        resolved[ObjectIdentifier(sender)] = entry
        publish()
    }

    func netService(_ sender: NetService, didNotResolve errorDict: [String: NSNumber]) {
        let fallback = DiscoveredSMBServer(serviceName: sender.name, host: sender.name, port: sender.port)
        resolved[ObjectIdentifier(sender)] = fallback
        publish()
    }
}
