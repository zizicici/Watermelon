import Foundation
import UniformTypeIdentifiers

final class MediaDropFileStagingStore: Sendable {
    enum StagingError: LocalizedError {
        case directoryNotSupported

        var errorDescription: String? {
            switch self {
            case .directoryNotSupported:
                return String(localized: "transfer.files.error.directory")
            }
        }
    }

    private let sessionDirectory = FileManager.default.temporaryDirectory
        .appendingPathComponent("MediaDrop", isDirectory: true)
        .appendingPathComponent(UUID().uuidString, isDirectory: true)

    deinit {
        let directory = sessionDirectory
        try? FileManager.default.removeItem(at: directory)
    }

    func stage(
        _ urls: [URL],
        excluding selectedFiles: [InboxTransferFile] = []
    ) async throws -> [InboxTransferFile] {
        let sessionDirectory = sessionDirectory
        let stagingTask = Task.detached(priority: .userInitiated) {
            let fileManager = FileManager.default
            try fileManager.createDirectory(
                at: sessionDirectory,
                withIntermediateDirectories: true
            )
            var stagedURLs: [URL] = []
            var knownFiles = selectedFiles
            do {
                var importedFiles: [InboxTransferFile] = []
                for sourceURL in urls {
                    try Task.checkCancellation()
                    let scoped = sourceURL.startAccessingSecurityScopedResource()
                    defer {
                        if scoped { sourceURL.stopAccessingSecurityScopedResource() }
                    }

                    let values = try sourceURL.resourceValues(forKeys: [
                        .isDirectoryKey,
                        .isRegularFileKey,
                        .nameKey,
                        .fileSizeKey,
                        .contentModificationDateKey,
                        .contentTypeKey,
                    ])
                    if values.isDirectory == true || values.isRegularFile == false {
                        throw StagingError.directoryNotSupported
                    }

                    let preferredName = values.name ?? sourceURL.lastPathComponent
                    let filenameExtension = (preferredName as NSString).pathExtension
                    let internalName = filenameExtension.isEmpty
                        ? UUID().uuidString
                        : "\(UUID().uuidString).\(filenameExtension)"
                    let destinationURL = sessionDirectory.appendingPathComponent(internalName)
                    var coordinationError: NSError?
                    var copyError: Error?
                    NSFileCoordinator().coordinate(
                        readingItemAt: sourceURL,
                        options: .withoutChanges,
                        error: &coordinationError
                    ) { coordinatedURL in
                        do {
                            try fileManager.copyItem(at: coordinatedURL, to: destinationURL)
                        } catch {
                            copyError = error
                        }
                    }
                    if let coordinationError { throw coordinationError }
                    if let copyError { throw copyError }
                    stagedURLs.append(destinationURL)
                    try Task.checkCancellation()
                    let stagedFile = InboxTransferFile(
                        id: UUID(),
                        localURL: destinationURL,
                        preferredName: preferredName,
                        fileSize: values.fileSize.map(Int64.init),
                        modificationDate: values.contentModificationDate,
                        contentTypeIdentifier: values.contentType?.identifier
                    )

                    if try Self.isDuplicate(stagedFile, among: knownFiles) {
                        try fileManager.removeItem(at: destinationURL)
                        stagedURLs.removeLast()
                        continue
                    }

                    knownFiles.append(stagedFile)
                    importedFiles.append(stagedFile)
                }
                return importedFiles
            } catch {
                for url in stagedURLs {
                    try? fileManager.removeItem(at: url)
                }
                throw error
            }
        }
        return try await withTaskCancellationHandler {
            try await stagingTask.value
        } onCancel: {
            stagingTask.cancel()
        }
    }

    private static func isDuplicate(
        _ candidate: InboxTransferFile,
        among files: [InboxTransferFile]
    ) throws -> Bool {
        for file in files where file.preferredName == candidate.preferredName {
            if let existingSize = file.fileSize,
               let candidateSize = candidate.fileSize,
               existingSize != candidateSize {
                continue
            }
            if try filesHaveEqualContents(file.localURL, candidate.localURL) {
                return true
            }
        }
        return false
    }

    private static func filesHaveEqualContents(_ lhsURL: URL, _ rhsURL: URL) throws -> Bool {
        let lhs = try FileHandle(forReadingFrom: lhsURL)
        let rhs = try FileHandle(forReadingFrom: rhsURL)
        defer {
            try? lhs.close()
            try? rhs.close()
        }

        let chunkSize = 1024 * 1024
        while true {
            try Task.checkCancellation()
            let lhsData = try lhs.read(upToCount: chunkSize) ?? Data()
            let rhsData = try rhs.read(upToCount: chunkSize) ?? Data()
            guard lhsData == rhsData else { return false }
            if lhsData.isEmpty { return true }
        }
    }

    func remove(_ file: InboxTransferFile) async {
        let sessionDirectory = sessionDirectory.standardizedFileURL
        let localURL = file.localURL.standardizedFileURL
        guard localURL.deletingLastPathComponent() == sessionDirectory else { return }
        await Task.detached(priority: .utility) {
            try? FileManager.default.removeItem(at: localURL)
        }.value
    }

    func removeAll() async {
        let sessionDirectory = sessionDirectory
        await Task.detached(priority: .utility) {
            try? FileManager.default.removeItem(at: sessionDirectory)
        }.value
    }
}
