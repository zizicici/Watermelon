import Foundation
import Network
import UIKit

final class ProfileReachabilityService: @unchecked Sendable {
    enum Reachability: Equatable {
        case unknown
        case reachable
        case unreachable
    }

    private struct Entry {
        var signature: ProbeSignature
        var reachability: Reachability
        var lastProbedAt: Date?
        var task: Task<Void, Never>?
    }

    private struct ProbeSignature: Hashable {
        let storageType: StorageType
        let host: String
        let port: Int
        let endpoint: String
        let bookmark: Data?
    }

    private static let throttleWindow: TimeInterval = 5
    private static let probeTimeout: TimeInterval = 3

    private let pathMonitor = NWPathMonitor()
    private let pathQueue = DispatchQueue(label: "watermelon.reachability.path")
    private let stateQueue = DispatchQueue(label: "watermelon.reachability.state")

    private var entries: [Int64: Entry] = [:]
    private var profilesByID: [Int64: ServerProfileRecord] = [:]
    private var activeProfileID: Int64?
    private var generation: UInt64 = 0
    private var foregroundObserver: NSObjectProtocol?
    private var started = false
    private var inflightRemaining = 0
    private var inflightAnyChanged = false

    var onChange: (@MainActor () -> Void)?

    func start() {
        stateQueue.async { [weak self] in
            guard let self, !self.started else { return }
            self.started = true
        }
        pathMonitor.pathUpdateHandler = { [weak self] _ in
            self?.sweep(force: true)
        }
        pathMonitor.start(queue: pathQueue)
        let observer = NotificationCenter.default.addObserver(
            forName: UIApplication.willEnterForegroundNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.sweep(force: true)
        }
        stateQueue.async { [weak self] in
            self?.foregroundObserver = observer
        }
    }

    deinit {
        pathMonitor.cancel()
        if let foregroundObserver {
            NotificationCenter.default.removeObserver(foregroundObserver)
        }
    }

    func setProfiles(_ profiles: [ServerProfileRecord], activeProfileID: Int64?) {
        stateQueue.async { [weak self] in
            guard let self else { return }
            var nextProfilesByID: [Int64: ServerProfileRecord] = [:]
            for profile in profiles {
                guard let id = profile.id else { continue }
                nextProfilesByID[id] = profile
            }
            let prevIDs = Set(self.profilesByID.keys)
            let nextIDs = Set(nextProfilesByID.keys)
            let setChanged = prevIDs != nextIDs
            let activeChanged = self.activeProfileID != activeProfileID
            var signatureChanged = false
            for (id, profile) in nextProfilesByID {
                guard let existing = self.entries[id] else { continue }
                let newSignature = Self.probeSignature(of: profile)
                if existing.signature != newSignature {
                    existing.task?.cancel()
                    self.entries.removeValue(forKey: id)
                    signatureChanged = true
                }
            }
            self.activeProfileID = activeProfileID
            for id in self.entries.keys where nextProfilesByID[id] == nil {
                self.entries[id]?.task?.cancel()
                self.entries.removeValue(forKey: id)
            }
            self.profilesByID = nextProfilesByID
            guard setChanged || activeChanged || signatureChanged else { return }
            self.sweepLocked(force: false)
        }
    }

    func reachability(for profileID: Int64) -> Reachability {
        stateQueue.sync {
            if activeProfileID == profileID { return .reachable }
            return entries[profileID]?.reachability ?? .unknown
        }
    }

    private func sweep(force: Bool) {
        stateQueue.async { [weak self] in
            self?.sweepLocked(force: force)
        }
    }

    private func sweepLocked(force: Bool) {
        generation &+= 1
        let gen = generation
        inflightRemaining = 0
        inflightAnyChanged = false
        let now = Date()
        var issued = 0
        for (id, profile) in profilesByID {
            if id == activeProfileID { continue }
            if !force,
               let last = entries[id]?.lastProbedAt,
               now.timeIntervalSince(last) < Self.throttleWindow {
                continue
            }
            entries[id]?.task?.cancel()
            var entry = entries[id] ?? Entry(
                signature: Self.probeSignature(of: profile),
                reachability: .unknown,
                lastProbedAt: nil,
                task: nil
            )
            let bookmarkData = profile.externalVolumeParams?.rootBookmarkData
            entry.task = Task.detached(priority: .utility) { [weak self] in
                let result = await Self.probe(profile: profile, externalBookmark: bookmarkData)
                self?.apply(result: result, for: id, generation: gen)
            }
            entries[id] = entry
            issued += 1
        }
        inflightRemaining = issued
    }

    private func apply(result: Reachability, for id: Int64, generation gen: UInt64) {
        stateQueue.async { [weak self] in
            guard let self, self.generation == gen, var entry = self.entries[id] else { return }
            let changed = entry.reachability != result
            entry.reachability = result
            entry.lastProbedAt = Date()
            entry.task = nil
            self.entries[id] = entry
            if changed { self.inflightAnyChanged = true }
            self.inflightRemaining -= 1
            if self.inflightRemaining <= 0 {
                let shouldNotify = self.inflightAnyChanged
                self.inflightAnyChanged = false
                self.inflightRemaining = 0
                if shouldNotify, let handler = self.onChange {
                    Task { @MainActor in
                        handler()
                    }
                }
            }
        }
    }

    private static func probeSignature(of profile: ServerProfileRecord) -> ProbeSignature {
        switch profile.resolvedStorageType {
        case .smb:
            return ProbeSignature(
                storageType: .smb,
                host: profile.host,
                port: profile.port == 0 ? 445 : profile.port,
                endpoint: "",
                bookmark: nil
            )
        case .webdav:
            return ProbeSignature(
                storageType: .webdav,
                host: "",
                port: 0,
                endpoint: profile.webDAVEndpointURLString ?? "",
                bookmark: nil
            )
        case .s3:
            return ProbeSignature(
                storageType: .s3,
                host: "",
                port: 0,
                endpoint: profile.s3DisplayURLString ?? "",
                bookmark: nil
            )
        case .externalVolume:
            return ProbeSignature(
                storageType: .externalVolume,
                host: "",
                port: 0,
                endpoint: "",
                bookmark: profile.externalVolumeParams?.rootBookmarkData
            )
        }
    }

    private static func probe(
        profile: ServerProfileRecord,
        externalBookmark: Data?
    ) async -> Reachability {
        switch profile.resolvedStorageType {
        case .smb:
            let port = profile.port == 0 ? 445 : profile.port
            return await probeTCP(host: profile.host, port: port)
        case .webdav:
            guard let url = profile.webDAVEndpointURL else { return .unreachable }
            return await probeHTTP(url: url)
        case .s3:
            guard let raw = profile.s3DisplayURLString,
                  let url = URL(string: raw) else { return .unreachable }
            return await probeHTTP(url: url)
        case .externalVolume:
            return probeExternal(bookmarkData: externalBookmark)
        }
    }

    private static func probeTCP(host: String, port: Int) async -> Reachability {
        guard !host.isEmpty,
              port > 0, port <= Int(UInt16.max),
              let nwPort = NWEndpoint.Port(rawValue: UInt16(port)) else {
            return .unreachable
        }
        let connection = NWConnection(host: NWEndpoint.Host(host), port: nwPort, using: .tcp)
        let result = await withCheckedContinuation { (continuation: CheckedContinuation<Reachability, Never>) in
            let resumed = ManagedAtomicFlag()
            connection.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    if resumed.set() { continuation.resume(returning: .reachable) }
                case .failed, .cancelled:
                    if resumed.set() { continuation.resume(returning: .unreachable) }
                default:
                    break
                }
            }
            connection.start(queue: .global(qos: .utility))
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + Self.probeTimeout) {
                if resumed.set() { continuation.resume(returning: .unreachable) }
            }
        }
        connection.cancel()
        return result
    }

    private static func probeHTTP(url: URL) async -> Reachability {
        var request = URLRequest(url: url)
        request.httpMethod = "HEAD"
        request.timeoutInterval = Self.probeTimeout
        do {
            let (_, response) = try await URLSession.shared.data(for: request)
            return response is HTTPURLResponse ? .reachable : .unreachable
        } catch {
            return .unreachable
        }
    }

    private static func probeExternal(bookmarkData: Data?) -> Reachability {
        guard let bookmarkData else { return .unreachable }
        let store = SecurityScopedBookmarkStore()
        do {
            let resolved = try store.resolveBookmarkData(bookmarkData)
            let started = resolved.url.startAccessingSecurityScopedResource()
            defer {
                if started { resolved.url.stopAccessingSecurityScopedResource() }
            }
            return (try? resolved.url.checkResourceIsReachable()) == true ? .reachable : .unreachable
        } catch {
            return .unreachable
        }
    }
}

private final class ManagedAtomicFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var value = false

    func set() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !value else { return false }
        value = true
        return true
    }
}
