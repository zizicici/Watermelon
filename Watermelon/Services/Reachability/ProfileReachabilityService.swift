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

    struct ProbeSignature: Hashable {
        let storageType: StorageType
        let host: String
        let port: Int
        let endpoint: String
        let bookmark: Data?
    }

    struct Hooks: Sendable {
        let now: @Sendable () -> Date
        let probe: @Sendable (ServerProfileRecord, Data?) async -> Reachability

        static let live = Hooks(
            now: Date.init,
            probe: { profile, bookmark in
                await ProfileReachabilityService.probe(
                    profile: profile,
                    externalBookmark: bookmark
                )
            }
        )
    }

    private static let throttleWindow: TimeInterval = 5
    private static let probeTimeout: TimeInterval = 3
    private static let periodicRefreshInterval: TimeInterval = 45

    private let pathMonitor = NWPathMonitor()
    private let pathQueue = DispatchQueue(label: "watermelon.reachability.path")
    private let stateQueue = DispatchQueue(label: "watermelon.reachability.state")
    private let hooks: Hooks

    private var entries: [Int64: Entry] = [:]
    private var profilesByID: [Int64: ServerProfileRecord] = [:]
    private var activeProfileID: Int64?
    private var generation: UInt64 = 0
    private var foregroundObserver: NSObjectProtocol?
    private var backgroundObserver: NSObjectProtocol?
    private var started = false
    private var isForeground = false
    private var inflightRemaining = 0
    private var inflightAnyChanged = false
    private var pendingSweepForce: Bool?

    private lazy var refreshScheduler = ProfileReachabilityRefreshScheduler.live(
        interval: Self.periodicRefreshInterval,
        refreshImmediately: { [weak self] in self?.resumeForeground() },
        refreshPeriodically: { [weak self] in self?.sweep(force: false) }
    )

    var onChange: (@MainActor () -> Void)?

    init(hooks: Hooks = .live) {
        self.hooks = hooks
    }

    func start() {
        let shouldStart = stateQueue.sync {
            guard !started else { return false }
            started = true
            return true
        }
        guard shouldStart else { return }
        pathMonitor.pathUpdateHandler = { [weak self] _ in
            self?.sweep(force: true)
        }
        pathMonitor.start(queue: pathQueue)
        let observer = NotificationCenter.default.addObserver(
            forName: UIApplication.willEnterForegroundNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.refreshScheduler.enterForeground()
        }
        let backgroundObserver = NotificationCenter.default.addObserver(
            forName: UIApplication.didEnterBackgroundNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.pauseForBackground()
        }
        stateQueue.async { [weak self] in
            self?.foregroundObserver = observer
            self?.backgroundObserver = backgroundObserver
        }
        DispatchQueue.main.async { [weak self] in
            guard UIApplication.shared.applicationState != .background else { return }
            self?.refreshScheduler.enterForeground()
        }
    }

    func stop() {
        refreshScheduler.stop()
        pathMonitor.cancel()
        let observers = stateQueue.sync { () -> [NSObjectProtocol] in
            isForeground = false
            cancelInflightLocked()
            let observers = [foregroundObserver, backgroundObserver].compactMap { $0 }
            foregroundObserver = nil
            backgroundObserver = nil
            return observers
        }
        observers.forEach { NotificationCenter.default.removeObserver($0) }
    }

    deinit {
        if started { refreshScheduler.stop() }
        pathMonitor.cancel()
        if let foregroundObserver {
            NotificationCenter.default.removeObserver(foregroundObserver)
        }
        if let backgroundObserver {
            NotificationCenter.default.removeObserver(backgroundObserver)
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
            self.cancelInflightLocked()
            self.sweepLocked(force: false)
        }
    }

    func reachability(for profileID: Int64) -> Reachability {
        stateQueue.sync {
            if activeProfileID == profileID { return .reachable }
            return entries[profileID]?.reachability ?? .unknown
        }
    }

    func sweep(force: Bool) {
        stateQueue.async { [weak self] in
            self?.sweepLocked(force: force)
        }
    }

    private func sweepLocked(force: Bool) {
        guard isForeground else { return }
        guard inflightRemaining == 0 else {
            pendingSweepForce = (pendingSweepForce ?? false) || force
            return
        }
        generation &+= 1
        let gen = generation
        inflightRemaining = 0
        inflightAnyChanged = false
        let now = hooks.now()
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
            let probe = hooks.probe
            entry.task = Task.detached(priority: .utility) { [weak self] in
                let result = await probe(profile, bookmarkData)
                self?.apply(result: result, for: id, generation: gen)
            }
            entries[id] = entry
            issued += 1
        }
        inflightRemaining = issued
    }

    func resumeForeground() {
        stateQueue.async { [weak self] in
            guard let self else { return }
            self.isForeground = true
            self.sweepLocked(force: true)
        }
    }

    func pauseForBackground() {
        refreshScheduler.enterBackground()
        stateQueue.sync {
            isForeground = false
            cancelInflightLocked()
        }
    }

    private func cancelInflightLocked() {
        generation &+= 1
        entries.values.forEach { $0.task?.cancel() }
        for id in entries.keys { entries[id]?.task = nil }
        inflightRemaining = 0
        inflightAnyChanged = false
        pendingSweepForce = nil
    }

    private func apply(result: Reachability, for id: Int64, generation gen: UInt64) {
        stateQueue.async { [weak self] in
            guard let self, self.generation == gen, var entry = self.entries[id] else { return }
            let changed = entry.reachability != result
            entry.reachability = result
            entry.lastProbedAt = self.hooks.now()
            entry.task = nil
            self.entries[id] = entry
            if changed { self.inflightAnyChanged = true }
            self.inflightRemaining -= 1
            if self.inflightRemaining <= 0 {
                let shouldNotify = self.inflightAnyChanged
                self.inflightAnyChanged = false
                self.inflightRemaining = 0
                let replayForce = self.pendingSweepForce
                self.pendingSweepForce = nil
                if shouldNotify, let handler = self.onChange {
                    Task { @MainActor in
                        handler()
                    }
                }
                if let replayForce {
                    self.sweepLocked(force: replayForce)
                }
            }
        }
    }

    static func probeSignature(of profile: ServerProfileRecord) -> ProbeSignature {
        switch profile.resolvedStorageType {
        case .smb:
            return ProbeSignature(
                storageType: .smb,
                host: operationalProbeHost(for: profile) ?? "",
                port: SMBEndpoint.effectivePort(profile.port),
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
        case .sftp:
            return ProbeSignature(
                storageType: .sftp,
                host: operationalProbeHost(for: profile) ?? "",
                port: SFTPEndpoint.effectivePort(profile.port),
                endpoint: "",
                bookmark: nil
            )
        case .onedrive:
            return ProbeSignature(
                storageType: .onedrive,
                host: "",
                port: 0,
                endpoint: OneDriveCloudEnvironment.global.graphBaseURL.absoluteString,
                bookmark: nil
            )
        }
    }

    private static func probe(
        profile: ServerProfileRecord,
        externalBookmark: Data?
    ) async -> Reachability {
        switch profile.resolvedStorageType {
        case .smb:
            guard let host = operationalProbeHost(for: profile) else { return .unreachable }
            let port = SMBEndpoint.effectivePort(profile.port)
            return await probeTCP(host: host, port: port)
        case .webdav:
            guard let url = profile.webDAVEndpointURL else { return .unreachable }
            return await probeHTTP(url: url)
        case .s3:
            guard let raw = profile.s3DisplayURLString,
                  let url = URL(string: raw) else { return .unreachable }
            return await probeHTTP(url: url)
        case .externalVolume:
            return probeExternal(bookmarkData: externalBookmark)
        case .sftp:
            guard let host = operationalProbeHost(for: profile) else { return .unreachable }
            let port = SFTPEndpoint.effectivePort(profile.port)
            return await probeTCP(host: host, port: port)
        case .onedrive:
            return await probeHTTP(url: OneDriveCloudEnvironment.global.graphBaseURL)
        }
    }

    static func operationalProbeHost(for profile: ServerProfileRecord) -> String? {
        switch profile.resolvedStorageType {
        case .smb:
            return RemoteHostEndpoint.socketHost(profile.host, strippingSMBScheme: true)
        case .sftp:
            return RemoteHostEndpoint.socketHost(profile.host)
        case .webdav, .s3, .externalVolume, .onedrive:
            return nil
        }
    }

    private static func probeTCP(host: String, port: Int) async -> Reachability {
        guard !host.isEmpty,
              port > 0, port <= Int(UInt16.max),
              let nwPort = NWEndpoint.Port(rawValue: UInt16(port)) else {
            return .unreachable
        }
        let connection = NWConnection(host: NWEndpoint.Host(host), port: nwPort, using: .tcp)
        let result = await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<Reachability, Never>) in
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
        } onCancel: {
            connection.cancel()
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

final class ProfileReachabilityRefreshScheduler: @unchecked Sendable {
    struct Hooks: Sendable {
        let scheduleRepeating: @Sendable (
            _ interval: TimeInterval,
            _ action: @escaping @Sendable () -> Void
        ) -> (@Sendable () -> Void)
        let refreshImmediately: @Sendable () -> Void
        let refreshPeriodically: @Sendable () -> Void
    }

    private let lock = NSLock()
    private let interval: TimeInterval
    private let hooks: Hooks
    private var isForeground = false
    private var cancelScheduledRefresh: (@Sendable () -> Void)?

    init(interval: TimeInterval, hooks: Hooks) {
        self.interval = interval
        self.hooks = hooks
    }

    static func live(
        interval: TimeInterval,
        refreshImmediately: @escaping @Sendable () -> Void,
        refreshPeriodically: @escaping @Sendable () -> Void
    ) -> ProfileReachabilityRefreshScheduler {
        ProfileReachabilityRefreshScheduler(
            interval: interval,
            hooks: Hooks(
                scheduleRepeating: { interval, action in
                    let timer = DispatchSource.makeTimerSource(queue: .global(qos: .utility))
                    timer.schedule(deadline: .now() + interval, repeating: interval)
                    timer.setEventHandler(handler: action)
                    timer.resume()
                    return {
                        timer.setEventHandler(handler: nil)
                        timer.cancel()
                    }
                },
                refreshImmediately: refreshImmediately,
                refreshPeriodically: refreshPeriodically
            )
        )
    }

    func enterForeground() {
        let previousCancellation = lock.withLock { () -> (@Sendable () -> Void)? in
            isForeground = true
            let previous = cancelScheduledRefresh
            cancelScheduledRefresh = nil
            return previous
        }
        previousCancellation?()
        hooks.refreshImmediately()
        let cancellation = hooks.scheduleRepeating(interval, hooks.refreshPeriodically)
        let shouldCancel = lock.withLock {
            guard isForeground, cancelScheduledRefresh == nil else { return true }
            cancelScheduledRefresh = cancellation
            return false
        }
        if shouldCancel { cancellation() }
    }

    func enterBackground() {
        stop()
    }

    func stop() {
        let cancellation = lock.withLock { () -> (@Sendable () -> Void)? in
            isForeground = false
            let cancellation = cancelScheduledRefresh
            cancelScheduledRefresh = nil
            return cancellation
        }
        cancellation?()
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
