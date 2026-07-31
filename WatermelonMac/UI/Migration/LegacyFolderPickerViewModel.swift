import Foundation

@MainActor
final class LegacyFolderPickerViewModel {
    enum LoadState: Equatable {
        case idle
        case loading
        case loaded
        case failed(String)
    }

    var onChange: (() -> Void)?

    private(set) var currentPath: String {
        didSet { notifyChange() }
    }
    private(set) var entries: [RemoteStorageEntry] = [] {
        didSet { notifyChange() }
    }
    private(set) var state: LoadState = .idle {
        didSet { notifyChange() }
    }

    private let client: any RemoteStorageClientProtocol
    private var loadTask: Task<Void, Never>?
    private var loadRequestID: UInt64 = 0

    init(client: any RemoteStorageClientProtocol, initialPath: String) {
        self.client = client
        self.currentPath = RemotePathBuilder.normalizePath(initialPath)
    }

    deinit {
        loadTask?.cancel()
    }

    func load() {
        loadTask?.cancel()
        loadRequestID &+= 1
        let requestID = loadRequestID
        state = .loading
        let target = currentPath
        loadTask = Task { [client] in
            do {
                let raw = try await client.list(path: target)
                try Task.checkCancellation()
                let dirs = raw
                    .filter { $0.isDirectory && $0.name != "." && $0.name != ".." }
                    .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
                await MainActor.run {
                    guard self.loadRequestID == requestID,
                          self.currentPath == target else {
                        return
                    }
                    self.loadTask = nil
                    self.entries = dirs
                    self.state = .loaded
                }
            } catch is CancellationError {
                return
            } catch {
                await MainActor.run {
                    guard self.loadRequestID == requestID,
                          self.currentPath == target else {
                        return
                    }
                    self.loadTask = nil
                    self.entries = []
                    self.state = .failed(error.localizedDescription)
                }
            }
        }
    }

    func navigate(to path: String) {
        currentPath = RemotePathBuilder.normalizePath(path)
        load()
    }

    func navigateUp() {
        guard currentPath != "/" else { return }
        let parent = (currentPath as NSString).deletingLastPathComponent
        navigate(to: parent.isEmpty ? "/" : parent)
    }

    var canGoUp: Bool { currentPath != "/" }

    private func notifyChange() {
        onChange?()
    }
}
