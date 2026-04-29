import Combine
import Foundation

@MainActor
final class LegacyFolderPickerViewModel: ObservableObject {
    enum LoadState: Equatable {
        case idle
        case loading
        case loaded
        case failed(String)
    }

    @Published private(set) var currentPath: String
    @Published private(set) var entries: [RemoteStorageEntry] = []
    @Published private(set) var state: LoadState = .idle

    private let client: any RemoteStorageClientProtocol
    private var loadTask: Task<Void, Never>?

    init(client: any RemoteStorageClientProtocol, initialPath: String) {
        self.client = client
        self.currentPath = RemotePathBuilder.normalizePath(initialPath)
    }

    func load() {
        loadTask?.cancel()
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
                    guard self.currentPath == target else { return }
                    self.entries = dirs
                    self.state = .loaded
                }
            } catch is CancellationError {
                return
            } catch {
                await MainActor.run {
                    guard self.currentPath == target else { return }
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
}
