import Combine
import Foundation

@MainActor
final class SMBSharePickerViewModel: ObservableObject {
    enum DirectoryLoadState: Equatable {
        case idle
        case loading
        case loaded
        case failed(String)
    }

    enum SharesLoadState: Equatable {
        case idle
        case loading
        case loaded
        case failed(String)
    }

    @Published private(set) var sharesState: SharesLoadState = .idle
    @Published private(set) var shares: [SMBShareInfo] = []
    @Published var selectedShareName: String?
    @Published private(set) var currentPath: String = "/"
    @Published private(set) var directoryState: DirectoryLoadState = .idle
    @Published private(set) var directoryEntries: [RemoteStorageEntry] = []

    private let auth: SMBServerAuthContext
    private let setupService = SMBSetupService()
    private var loadTask: Task<Void, Never>?

    init(auth: SMBServerAuthContext) {
        self.auth = auth
    }

    var canCommit: Bool {
        guard selectedShareName != nil else { return false }
        if case .loaded = directoryState { return true }
        return false
    }

    var commitContext: SMBServerPathContext? {
        guard let name = selectedShareName else { return nil }
        return SMBServerPathContext(auth: auth, shareName: name, basePath: currentPath)
    }

    func loadShares() {
        sharesState = .loading
        Task {
            do {
                let result = try await setupService.listShares(auth: auth)
                let unique = Self.uniqueByName(result)
                await MainActor.run {
                    self.shares = unique
                    self.sharesState = .loaded
                }
            } catch {
                await MainActor.run {
                    self.shares = []
                    self.sharesState = .failed(error.localizedDescription)
                }
            }
        }
    }

    func selectShare(_ name: String) {
        selectedShareName = name
        currentPath = "/"
        directoryEntries = []
        loadDirectory()
    }

    func navigate(to path: String) {
        currentPath = RemotePathBuilder.normalizePath(path)
        loadDirectory()
    }

    func navigateUp() {
        let normalized = RemotePathBuilder.normalizePath(currentPath)
        guard normalized != "/" else { return }
        let parent = (normalized as NSString).deletingLastPathComponent
        navigate(to: parent.isEmpty ? "/" : parent)
    }

    func retryDirectory() {
        loadDirectory()
    }

    private func loadDirectory() {
        guard let shareName = selectedShareName else { return }
        loadTask?.cancel()
        directoryState = .loading
        let target = currentPath
        loadTask = Task { [auth, setupService] in
            do {
                let dirs = try await setupService.listDirectories(
                    auth: auth,
                    shareName: shareName,
                    path: target
                )
                try Task.checkCancellation()
                await MainActor.run {
                    guard self.selectedShareName == shareName, self.currentPath == target else { return }
                    self.directoryEntries = dirs
                    self.directoryState = .loaded
                }
            } catch is CancellationError {
                return
            } catch {
                await MainActor.run {
                    guard self.selectedShareName == shareName, self.currentPath == target else { return }
                    self.directoryEntries = []
                    self.directoryState = .failed(error.localizedDescription)
                }
            }
        }
    }

    private static func uniqueByName(_ shares: [SMBShareInfo]) -> [SMBShareInfo] {
        var seen = Set<String>()
        return shares.filter { seen.insert($0.name).inserted }
    }
}
