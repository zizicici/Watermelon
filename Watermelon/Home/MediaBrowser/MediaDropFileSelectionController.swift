import UIKit
import UniformTypeIdentifiers

enum MediaDropFileIcon {
    static func image(for file: InboxTransferFile) -> UIImage? {
        guard let identifier = file.contentTypeIdentifier,
              let type = UTType(identifier) else {
            return UIImage(systemName: "doc")
        }
        let symbolName: String
        if type.conforms(to: .image) {
            symbolName = "photo"
        } else if type.conforms(to: .movie) || type.conforms(to: .video) {
            symbolName = "video"
        } else if type.conforms(to: .audio) {
            symbolName = "waveform"
        } else if type.conforms(to: .pdf) {
            symbolName = "doc.richtext"
        } else if type.conforms(to: .archive) {
            symbolName = "archivebox"
        } else {
            symbolName = "doc"
        }
        return UIImage(systemName: symbolName)
    }
}

@MainActor
final class MediaDropFileSelectionController {
    private let stagingStore: MediaDropFileStagingStore
    private var importTask: Task<Void, Never>?
    private(set) var files: [InboxTransferFile] = []
    private(set) var isWorking = false

    var onChange: (() -> Void)?
    var onError: ((Error) -> Void)?

    init(stagingStore: MediaDropFileStagingStore = MediaDropFileStagingStore()) {
        self.stagingStore = stagingStore
    }

    deinit {
        importTask?.cancel()
    }

    func importFiles(_ urls: [URL]) {
        guard !urls.isEmpty, !isWorking else { return }
        isWorking = true
        onChange?()
        importTask = Task { @MainActor [weak self, stagingStore] in
            do {
                let imported = try await stagingStore.stage(urls, excluding: self?.files ?? [])
                if Task.isCancelled {
                    for file in imported {
                        await stagingStore.remove(file)
                    }
                } else {
                    self?.files.append(contentsOf: imported)
                }
            } catch is CancellationError {
            } catch {
                self?.onError?(error)
            }
            guard let self else { return }
            self.importTask = nil
            self.isWorking = false
            self.onChange?()
        }
    }

    func resetAfterSuccessfulTransfer() {
        guard !isWorking, !files.isEmpty else { return }
        files.removeAll()
        isWorking = true
        onChange?()
        importTask = Task { @MainActor [weak self, stagingStore] in
            await stagingStore.removeAll()
            guard let self else { return }
            self.importTask = nil
            self.isWorking = false
            self.onChange?()
        }
    }

    func cancelPendingImport() {
        importTask?.cancel()
    }

    func remove(_ file: InboxTransferFile) {
        guard !isWorking else { return }
        files.removeAll { $0.id == file.id }
        onChange?()
        Task { await stagingStore.remove(file) }
    }

    func removeAll() {
        guard !isWorking, !files.isEmpty else { return }
        files.removeAll()
        isWorking = true
        onChange?()
        importTask = Task { @MainActor [weak self, stagingStore] in
            await stagingStore.removeAll()
            guard let self else { return }
            self.importTask = nil
            self.isWorking = false
            self.onChange?()
        }
    }
}
