import Foundation

@MainActor
final class MediaBrowserReloadScheduler {
    struct Request {
        let trigger: String
        let staleAttempt: Int
    }

    private let delay: TimeInterval
    private let reload: (Request) -> Void
    private var pending: (request: Request, delay: TimeInterval)?
    private var timer: Task<Void, Never>?
    private var isActive = false
    private var isInteractionBlocked = false
    private var isLoading = false

    init(delay: TimeInterval = 0.25, reload: @escaping (Request) -> Void) {
        self.delay = delay
        self.reload = reload
    }

    deinit { timer?.cancel() }

    func request(_ request: Request, delay: TimeInterval? = nil) {
        let interval = max(0, delay ?? self.delay)
        pending = (request, min(pending?.delay ?? interval, interval))
        if interval == 0 {
            cancelTimer()
        }
        schedule()
    }

    func update(isActive: Bool, isInteractionBlocked: Bool, isLoading: Bool = false) {
        self.isActive = isActive
        self.isInteractionBlocked = isInteractionBlocked
        self.isLoading = isLoading
        if !canReload {
            cancelTimer()
        } else {
            schedule()
        }
    }

    private var canReload: Bool {
        pending != nil && isActive && !isInteractionBlocked && !isLoading
    }

    private func schedule() {
        guard canReload, timer == nil, let interval = pending?.delay else { return }
        timer = Task { [weak self] in
            if interval > 0 {
                do { try await Task.sleep(for: .seconds(interval)) } catch { return }
            }
            guard !Task.isCancelled else { return }
            self?.drain()
        }
    }

    private func cancelTimer() {
        timer?.cancel()
        timer = nil
    }

    private func drain() {
        timer = nil
        guard canReload, let request = pending?.request else { return }
        pending = nil
        reload(request)
    }
}
