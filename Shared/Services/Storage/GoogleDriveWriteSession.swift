import Foundation

nonisolated struct GoogleDriveWriteSessionKey: Hashable, Sendable {
    let accountSubject: String
    let rootFolderID: String
}

nonisolated struct GoogleDriveWriteUploadTicket: Sendable {
    let generation: UUID
    let token: UUID
    let parentPath: String
    let name: String
    let parent: GoogleDriveFile
    let existing: GoogleDriveFile?
}

nonisolated struct GoogleDriveBoundUpload: Sendable {
    let ticket: GoogleDriveWriteUploadTicket
    let itemID: String
    let expectedSize: Int64
}

nonisolated enum GoogleDriveWriteUploadPreparation: Sendable {
    case unavailable
    case busy
    case occupied
    case recover(GoogleDriveBoundUpload)
    case ready(GoogleDriveWriteUploadTicket)
}

nonisolated enum GoogleDriveWriteSessionLookup: Sendable {
    case unavailable
    case busy
    case missing
    case file(GoogleDriveFile)
}

actor GoogleDriveWriteSessionState {
    private struct UploadOperation: Sendable {
        let ticket: GoogleDriveWriteUploadTicket
        var itemID: String?
        var expectedSize: Int64?
        var expectedMD5: String?
    }

    private enum Entry: Sendable {
        case present(GoogleDriveFile)
        case uploading(UploadOperation)
        case uncertain(UploadOperation)
    }

    private struct DirectoryState: Sendable {
        var folder: GoogleDriveFile
        var isLoaded: Bool
        var entriesByName: [String: Entry]
    }

    private struct SessionState {
        let generation: UUID
        var directoriesByPath: [String: DirectoryState]
    }

    private var sessionsByKey: [GoogleDriveWriteSessionKey: SessionState] = [:]

    func begin(for key: GoogleDriveWriteSessionKey, root: GoogleDriveFile?) -> UUID {
        let generation = UUID()
        var directories: [String: DirectoryState] = [:]
        if let root {
            directories["/"] = DirectoryState(folder: root, isLoaded: false, entriesByName: [:])
        }
        sessionsByKey[key] = SessionState(
            generation: generation,
            directoriesByPath: directories
        )
        return generation
    }

    func end(for key: GoogleDriveWriteSessionKey, generation: UUID) {
        guard sessionsByKey[key]?.generation == generation else { return }
        sessionsByKey.removeValue(forKey: key)
    }

    func generation(for key: GoogleDriveWriteSessionKey) -> UUID? {
        sessionsByKey[key]?.generation
    }

    func observe(
        _ file: GoogleDriveFile,
        path: String,
        key: GoogleDriveWriteSessionKey,
        generation: UUID
    ) {
        guard var session = sessionsByKey[key], session.generation == generation else { return }
        if path == "/" {
            if session.directoriesByPath[path]?.folder.id != file.id {
                session.directoriesByPath.removeAll()
            }
            var root = session.directoriesByPath[path]
                ?? DirectoryState(folder: file, isLoaded: false, entriesByName: [:])
            root.folder = file
            session.directoriesByPath[path] = root
            sessionsByKey[key] = session
            return
        }
        if let (parentPath, name) = Self.parentAndName(of: path),
           var parent = session.directoriesByPath[parentPath] {
            switch parent.entriesByName[name] {
            case .present(let current):
                if current.mimeType == GoogleDriveConstants.folderMIMEType,
                   current.id != file.id {
                    Self.removeDirectorySubtree(path, from: &session)
                }
            case .uploading, .uncertain:
                return
            case nil:
                break
            }
            parent.entriesByName[name] = .present(file)
            session.directoriesByPath[parentPath] = parent
        }
        if file.mimeType == GoogleDriveConstants.folderMIMEType {
            if var directory = session.directoriesByPath[path], directory.folder.id == file.id {
                directory.folder = file
                session.directoriesByPath[path] = directory
            } else {
                Self.removeDirectorySubtree(path, from: &session)
                session.directoriesByPath[path] = DirectoryState(
                    folder: file,
                    isLoaded: false,
                    entriesByName: [:]
                )
            }
        }
        sessionsByKey[key] = session
    }

    func lookup(
        path: String,
        key: GoogleDriveWriteSessionKey,
        generation: UUID
    ) -> GoogleDriveWriteSessionLookup {
        guard let session = sessionsByKey[key], session.generation == generation else {
            return .unavailable
        }
        if path == "/", let root = session.directoriesByPath[path]?.folder {
            return .file(root)
        }
        guard let (parentPath, name) = Self.parentAndName(of: path),
              let parent = session.directoriesByPath[parentPath] else {
            return .unavailable
        }
        if let entry = parent.entriesByName[name] {
            switch entry {
            case .present(let file):
                return .file(file)
            case .uploading, .uncertain:
                return .busy
            }
        }
        return parent.isLoaded ? .missing : .unavailable
    }

    func isDirectoryLoaded(
        path: String,
        key: GoogleDriveWriteSessionKey,
        generation: UUID
    ) -> Bool {
        guard let session = sessionsByKey[key], session.generation == generation else { return false }
        return session.directoriesByPath[path]?.isLoaded == true
    }

    func install(
        path: String,
        folder: GoogleDriveFile,
        childrenByName: [String: GoogleDriveFile],
        key: GoogleDriveWriteSessionKey,
        generation: UUID
    ) throws {
        guard var session = sessionsByKey[key], session.generation == generation else { return }
        let previous = session.directoriesByPath[path]
        var recoveredEntries: [String: Entry] = [:]
        if let previous {
            for (name, entry) in previous.entriesByName {
                switch entry {
                case .uploading:
                    throw RemoteStorageClientError.unavailable
                case .uncertain(let operation):
                    recoveredEntries[name] = try Self.recoverUncertain(
                        operation,
                        name: name,
                        childrenByName: childrenByName
                    )
                case .present:
                    break
                }
                if case .present(let file) = entry,
                   file.mimeType == GoogleDriveConstants.folderMIMEType,
                   childrenByName[name]?.id != file.id {
                    Self.removeDirectorySubtree(Self.appending(name, to: path), from: &session)
                }
            }
        }
        if previous?.folder.id != nil, previous?.folder.id != folder.id {
            Self.removeDirectorySubtree(path, from: &session)
        }
        var entries: [String: Entry] = [:]
        for (name, file) in childrenByName {
            entries[name] = .present(file)
        }
        for (name, entry) in recoveredEntries {
            entries[name] = entry
        }
        session.directoriesByPath[path] = DirectoryState(
            folder: folder,
            isLoaded: true,
            entriesByName: entries
        )
        for (name, file) in childrenByName where file.mimeType == GoogleDriveConstants.folderMIMEType {
            let childPath = Self.appending(name, to: path)
            if var directory = session.directoriesByPath[childPath], directory.folder.id == file.id {
                directory.folder = file
                session.directoriesByPath[childPath] = directory
            } else {
                Self.removeDirectorySubtree(childPath, from: &session)
                session.directoriesByPath[childPath] = DirectoryState(
                    folder: file,
                    isLoaded: false,
                    entriesByName: [:]
                )
            }
        }
        sessionsByKey[key] = session
    }

    func prepareUpload(
        parentPath: String,
        name: String,
        mode: RemoteUploadMode,
        key: GoogleDriveWriteSessionKey,
        generation: UUID
    ) -> GoogleDriveWriteUploadPreparation {
        guard var session = sessionsByKey[key], session.generation == generation,
              var directory = session.directoriesByPath[parentPath],
              directory.isLoaded else {
            return .unavailable
        }
        let previous: GoogleDriveFile?
        switch directory.entriesByName[name] {
        case .present(let file):
            if file.mimeType == GoogleDriveConstants.folderMIMEType { return .occupied }
            if mode == .createIfAbsent { return .occupied }
            previous = file
        case .uploading:
            return .busy
        case .uncertain(let operation):
            guard let itemID = operation.itemID, let expectedSize = operation.expectedSize else {
                return .busy
            }
            return .recover(GoogleDriveBoundUpload(
                ticket: operation.ticket,
                itemID: itemID,
                expectedSize: expectedSize
            ))
        case nil:
            previous = nil
        }
        let token = UUID()
        let ticket = GoogleDriveWriteUploadTicket(
            generation: generation,
            token: token,
            parentPath: parentPath,
            name: name,
            parent: directory.folder,
            existing: previous
        )
        directory.entriesByName[name] = .uploading(UploadOperation(
            ticket: ticket,
            itemID: nil,
            expectedSize: nil,
            expectedMD5: nil
        ))
        session.directoriesByPath[parentPath] = directory
        sessionsByKey[key] = session
        return .ready(ticket)
    }

    func bindUpload(
        _ ticket: GoogleDriveWriteUploadTicket,
        itemID: String,
        expectedSize: Int64,
        key: GoogleDriveWriteSessionKey
    ) -> GoogleDriveBoundUpload? {
        guard var session = sessionsByKey[key], session.generation == ticket.generation,
              var directory = session.directoriesByPath[ticket.parentPath],
              case .uploading(var operation)? = directory.entriesByName[ticket.name],
              operation.ticket.token == ticket.token else { return nil }
        operation.itemID = itemID
        operation.expectedSize = expectedSize
        directory.entriesByName[ticket.name] = .uploading(operation)
        session.directoriesByPath[ticket.parentPath] = directory
        sessionsByKey[key] = session
        return GoogleDriveBoundUpload(ticket: ticket, itemID: itemID, expectedSize: expectedSize)
    }

    func completeUpload(
        _ upload: GoogleDriveBoundUpload,
        file: GoogleDriveFile,
        key: GoogleDriveWriteSessionKey
    ) -> Bool {
        guard var session = sessionsByKey[key], session.generation == upload.ticket.generation,
              var directory = session.directoriesByPath[upload.ticket.parentPath],
              let entry = directory.entriesByName[upload.ticket.name] else { return false }
        let operation: UploadOperation
        switch entry {
        case .uploading(let current), .uncertain(let current):
            operation = current
        case .present:
            return false
        }
        guard operation.ticket.token == upload.ticket.token,
              operation.itemID == upload.itemID else { return false }
        directory.entriesByName[upload.ticket.name] = .present(file)
        session.directoriesByPath[upload.ticket.parentPath] = directory
        sessionsByKey[key] = session
        return true
    }

    func markUploadUncertain(
        _ upload: GoogleDriveBoundUpload,
        expectedMD5: String?,
        key: GoogleDriveWriteSessionKey
    ) {
        guard var session = sessionsByKey[key], session.generation == upload.ticket.generation,
              var directory = session.directoriesByPath[upload.ticket.parentPath],
              let entry = directory.entriesByName[upload.ticket.name] else { return }
        var operation: UploadOperation
        switch entry {
        case .uploading(let current), .uncertain(let current):
            operation = current
        case .present:
            return
        }
        guard operation.ticket.token == upload.ticket.token,
              operation.itemID == upload.itemID else { return }
        operation.expectedMD5 = expectedMD5
        directory.entriesByName[upload.ticket.name] = .uncertain(operation)
        session.directoriesByPath[upload.ticket.parentPath] = directory
        sessionsByKey[key] = session
    }

    func cancelUpload(
        _ ticket: GoogleDriveWriteUploadTicket,
        key: GoogleDriveWriteSessionKey
    ) {
        guard var session = sessionsByKey[key], session.generation == ticket.generation,
              var directory = session.directoriesByPath[ticket.parentPath],
              let entry = directory.entriesByName[ticket.name] else { return }
        let operation: UploadOperation
        switch entry {
        case .uploading(let current), .uncertain(let current):
            operation = current
        case .present:
            return
        }
        guard operation.ticket.token == ticket.token else { return }
        Self.restore(operation, name: ticket.name, in: &directory)
        session.directoriesByPath[ticket.parentPath] = directory
        sessionsByKey[key] = session
    }

    func remove(
        path: String,
        key: GoogleDriveWriteSessionKey,
        generation: UUID
    ) {
        guard var session = sessionsByKey[key], session.generation == generation else { return }
        if let (parentPath, name) = Self.parentAndName(of: path),
           var parent = session.directoriesByPath[parentPath] {
            parent.entriesByName.removeValue(forKey: name)
            session.directoriesByPath[parentPath] = parent
        }
        Self.removeDirectorySubtree(path, from: &session)
        sessionsByKey[key] = session
    }

    func applyMove(
        _ file: GoogleDriveFile,
        from sourcePath: String,
        to destinationPath: String,
        key: GoogleDriveWriteSessionKey,
        generation: UUID
    ) {
        guard var session = sessionsByKey[key], session.generation == generation else { return }
        if let (sourceParentPath, sourceName) = Self.parentAndName(of: sourcePath),
           var sourceParent = session.directoriesByPath[sourceParentPath] {
            sourceParent.entriesByName.removeValue(forKey: sourceName)
            session.directoriesByPath[sourceParentPath] = sourceParent
        }
        Self.removeDirectorySubtree(sourcePath, from: &session)
        if let (destinationParentPath, destinationName) = Self.parentAndName(of: destinationPath),
           var destinationParent = session.directoriesByPath[destinationParentPath] {
            destinationParent.entriesByName[destinationName] = .present(file)
            session.directoriesByPath[destinationParentPath] = destinationParent
        }
        if file.mimeType == GoogleDriveConstants.folderMIMEType {
            session.directoriesByPath[destinationPath] = DirectoryState(
                folder: file,
                isLoaded: false,
                entriesByName: [:]
            )
        }
        sessionsByKey[key] = session
    }

    private static func parentAndName(of path: String) -> (String, String)? {
        guard path != "/", let split = path.lastIndex(of: "/") else { return nil }
        let parent = split == path.startIndex ? "/" : String(path[..<split])
        let name = String(path[path.index(after: split)...])
        return (parent, name)
    }

    private static func appending(_ name: String, to path: String) -> String {
        path == "/" ? "/" + name : path + "/" + name
    }

    private static func restore(
        _ operation: UploadOperation,
        name: String,
        in directory: inout DirectoryState
    ) {
        if let previous = operation.ticket.existing {
            directory.entriesByName[name] = .present(previous)
        } else {
            directory.entriesByName.removeValue(forKey: name)
        }
    }

    private static func recoverUncertain(
        _ operation: UploadOperation,
        name: String,
        childrenByName: [String: GoogleDriveFile]
    ) throws -> Entry {
        guard let itemID = operation.itemID,
              let expectedSize = operation.expectedSize else {
            throw RemoteStorageClientError.unavailable
        }
        guard let current = childrenByName[name] else { return .uncertain(operation) }
        guard current.id == itemID else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        if let expectedMD5 = operation.expectedMD5,
           current.size.flatMap(Int64.init) == expectedSize,
           current.md5Checksum?.caseInsensitiveCompare(expectedMD5) == .orderedSame {
            return .present(current)
        }
        if let previous = operation.ticket.existing,
           current.hasSameContentsAndBinding(as: previous) {
            return .present(previous)
        }
        throw RemoteStorageClientError.unavailable
    }

    private static func removeDirectorySubtree(_ path: String, from session: inout SessionState) {
        let prefix = path + "/"
        session.directoriesByPath = session.directoriesByPath.filter { candidate, _ in
            candidate != path && !candidate.hasPrefix(prefix)
        }
    }
}
