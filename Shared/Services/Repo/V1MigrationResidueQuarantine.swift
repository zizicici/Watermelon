import Foundation
import os.log

private let v1MigrationResidueQuarantineLog = Logger(subsystem: "com.zizicici.watermelon", category: "V1MigrationResidueQuarantine")

nonisolated struct V1MigrationResidueQuarantine: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    /// Idempotent rename so detectV1Manifests stops routing the month as `.v1`.
    func quarantine(year: Int, month: Int, sourcePath: String) async throws {
        let monthRel = String(format: "%04d/%02d", year, month)
        let residuePath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRel + "/" + V1MigrationResidueFileNames.residueManifestFileName
        )
        if let meta = try await metadataIfPresent(path: residuePath), !meta.isDirectory {
            guard try await metadataIfPresent(path: sourcePath) != nil else { return }
            if try await remoteFilesEqual(sourcePath, residuePath) {
                try await deleteIfPresent(path: sourcePath)
                return
            }
            try await moveSourceToUniqueResidue(monthRel: monthRel, sourcePath: sourcePath)
            return
        }
        // Peer-deletion between scan and quarantine must not abort the whole phase.
        guard try await metadataIfPresent(path: sourcePath) != nil else { return }
        if try await !client.resolvedSupportsExclusiveMoveIfAbsent(forDestinationPath: residuePath) {
            try await copySourceToUniqueResidue(monthRel: monthRel, sourcePath: sourcePath)
            return
        }
        do {
            switch try await client.moveIfAbsent(from: sourcePath, to: residuePath) {
            case .created:
                return
            case .bestEffortRetry:
                try await finishBestEffortResidueMove(sourcePath: sourcePath, destinationPath: residuePath)
                return
            case .alreadyExists:
                if try await remoteFilesEqual(sourcePath, residuePath) {
                    try await deleteIfPresent(path: sourcePath)
                } else {
                    try await moveSourceToUniqueResidue(monthRel: monthRel, sourcePath: sourcePath)
                }
                return
            }
        } catch {
            if isStorageNotFoundError(error) { return }
            if let meta = try await metadataIfPresent(path: residuePath), !meta.isDirectory {
                guard try await metadataIfPresent(path: sourcePath) != nil else { return }
                if try await remoteFilesEqual(sourcePath, residuePath) {
                    try await deleteIfPresent(path: sourcePath)
                } else {
                    try await moveSourceToUniqueResidue(monthRel: monthRel, sourcePath: sourcePath)
                }
                return
            }
            throw error
        }
    }

    func sweepResidueManifests() async throws {
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: basePath)
        } catch {
            if isStorageNotFoundError(error) { return }
            throw error
        }
        let yearEntries = entries.filter { $0.isDirectory && $0.name.range(of: "^[0-9]{4}$", options: .regularExpression) != nil }
        for yearEntry in yearEntries {
            try Task.checkCancellation()
            let yearPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: yearEntry.name)
            let monthEntries: [RemoteStorageEntry]
            do {
                monthEntries = try await client.list(path: yearPath)
            } catch {
                if isStorageNotFoundError(error) { continue }
                throw error
            }
            for monthEntry in monthEntries where monthEntry.isDirectory && monthEntry.name.range(of: "^[0-9]{2}$", options: .regularExpression) != nil {
                try Task.checkCancellation()
                let monthPath = RemotePathBuilder.absolutePath(basePath: yearPath, remoteRelativePath: monthEntry.name)
                let files: [RemoteStorageEntry]
                do {
                    files = try await client.list(path: monthPath)
                } catch {
                    if isStorageNotFoundError(error) { continue }
                    throw error
                }
                // Treat ANY entry at the partial-marker path (file OR directory) as a
                // preservation gate. A directory at the reserved marker path is damaged
                // remote state, not proof of marker absence, so retention preflights downstream
                // need the residue evidence preserved.
                let partialMarkerEntry = files.first(where: { $0.name == V1MigrationResidueFileNames.partialMigrationMarkerFileName })
                let residueFiles = files.filter { !$0.isDirectory && Self.isResidueManifestName($0.name) }
                if partialMarkerEntry != nil && !residueFiles.isEmpty {
                    if partialMarkerEntry?.isDirectory == true {
                        v1MigrationResidueQuarantineLog.warning(
                            "preserving \(residueFiles.count, privacy: .public) V1 residue manifest(s) under directory-shaped partial migration marker at \(monthPath, privacy: .public)"
                        )
                    } else {
                        v1MigrationResidueQuarantineLog.info(
                            "preserving \(residueFiles.count, privacy: .public) V1 residue manifest(s) under partial migration marker at \(monthPath, privacy: .public)"
                        )
                    }
                    continue
                }
                for file in residueFiles {
                    try Task.checkCancellation()
                    try await deleteIfPresent(path: file.path)
                }
            }
        }
    }

    private func moveSourceToUniqueResidue(monthRel: String, sourcePath: String) async throws {
        let uniqueResiduePath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRel + "/" + V1MigrationResidueFileNames.residueManifestFileName + ".\(UUID().uuidString)"
        )
        if try await !client.resolvedSupportsExclusiveMoveIfAbsent(forDestinationPath: uniqueResiduePath) {
            try await copySourceToVerifiedResidue(sourcePath: sourcePath, destinationPath: uniqueResiduePath)
            return
        }
        do {
            switch try await client.moveIfAbsent(from: sourcePath, to: uniqueResiduePath) {
            case .created:
                return
            case .bestEffortRetry:
                try await finishBestEffortResidueMove(sourcePath: sourcePath, destinationPath: uniqueResiduePath)
                return
            case .alreadyExists:
                throw NSError(domain: "V1MigrationService", code: -32, userInfo: [
                    NSLocalizedDescriptionKey: "unique residue path already exists for \(sourcePath)"
                ])
            }
        } catch {
            if isStorageNotFoundError(error) { return }
            throw error
        }
    }

    private func copySourceToUniqueResidue(monthRel: String, sourcePath: String) async throws {
        for _ in 0..<4 {
            let uniqueResiduePath = RemotePathBuilder.absolutePath(
                basePath: basePath,
                remoteRelativePath: monthRel + "/" + V1MigrationResidueFileNames.residueManifestFileName + ".\(UUID().uuidString)"
            )
            guard try await metadataIfPresent(path: uniqueResiduePath) == nil else { continue }
            try await copySourceToVerifiedResidue(sourcePath: sourcePath, destinationPath: uniqueResiduePath)
            return
        }
        throw NSError(domain: "V1MigrationService", code: -34, userInfo: [
            NSLocalizedDescriptionKey: "could not allocate unique residue path for \(sourcePath)"
        ])
    }

    private func copySourceToVerifiedResidue(sourcePath: String, destinationPath: String) async throws {
        guard try await metadataIfPresent(path: sourcePath) != nil else { return }
        do {
            try await client.copy(from: sourcePath, to: destinationPath)
        } catch {
            if isStorageNotFoundError(error) { return }
            throw error
        }
        guard try await remoteFilesEqual(sourcePath, destinationPath) else {
            try? await deleteIfPresent(path: destinationPath)
            if try await metadataIfPresent(path: sourcePath) == nil { return }
            throw residueMoveIncompleteError(sourcePath: sourcePath, destinationPath: destinationPath)
        }
        do {
            try await client.delete(path: sourcePath)
        } catch {
            if isStorageNotFoundError(error) { return }
            throw error
        }
        guard try await metadataIfPresent(path: sourcePath) == nil else {
            throw residueMoveIncompleteError(sourcePath: sourcePath, destinationPath: destinationPath)
        }
    }

    private func finishBestEffortResidueMove(sourcePath: String, destinationPath: String) async throws {
        guard try await metadataIfPresent(path: sourcePath) != nil else { return }
        guard try await remoteFilesEqual(sourcePath, destinationPath) else {
            if try await metadataIfPresent(path: sourcePath) == nil { return }
            throw residueMoveIncompleteError(sourcePath: sourcePath, destinationPath: destinationPath)
        }
        do {
            try await client.delete(path: sourcePath)
        } catch {
            if isStorageNotFoundError(error) { return }
            throw error
        }
        guard try await metadataIfPresent(path: sourcePath) == nil else {
            throw residueMoveIncompleteError(sourcePath: sourcePath, destinationPath: destinationPath)
        }
    }

    private func residueMoveIncompleteError(sourcePath: String, destinationPath: String) -> NSError {
        NSError(domain: "V1MigrationService", code: -33, userInfo: [
            NSLocalizedDescriptionKey: "V1 manifest quarantine incomplete: source still visible at \(sourcePath) after moving to \(destinationPath)"
        ])
    }

    private static func isResidueManifestName(_ name: String) -> Bool {
        name == V1MigrationResidueFileNames.residueManifestFileName || name.hasPrefix(V1MigrationResidueFileNames.residueManifestFileName + ".")
    }

    private func metadataIfPresent(path: String) async throws -> RemoteStorageEntry? {
        do {
            return try await client.metadata(path: path)
        } catch {
            if isStorageNotFoundError(error) { return nil }
            throw error
        }
    }

    private func deleteIfPresent(path: String) async throws {
        guard try await metadataIfPresent(path: path) != nil else { return }
        do {
            try await client.delete(path: path)
        } catch {
            // A peer racing the same cleanup can remove the file between metadata
            // and delete; SMB/WebDAV/SFTP surface that as an error (S3 is idempotent).
            // Treat not-found as success so cleanup doesn't abort with a spurious error.
            if !isStorageNotFoundError(error) { throw error }
        }
    }

    private func remoteFilesEqual(_ lhsPath: String, _ rhsPath: String) async throws -> Bool {
        guard let lhsMeta = try await metadataIfPresent(path: lhsPath), !lhsMeta.isDirectory,
              let rhsMeta = try await metadataIfPresent(path: rhsPath), !rhsMeta.isDirectory,
              lhsMeta.size == rhsMeta.size else {
            return false
        }
        let lhsURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1-residue-compare-\(UUID().uuidString)-lhs.sqlite")
        let rhsURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1-residue-compare-\(UUID().uuidString)-rhs.sqlite")
        defer {
            try? FileManager.default.removeItem(at: lhsURL)
            try? FileManager.default.removeItem(at: rhsURL)
        }
        do {
            try await client.download(remotePath: lhsPath, localURL: lhsURL)
            try await client.download(remotePath: rhsPath, localURL: rhsURL)
        } catch {
            if isStorageNotFoundError(error) { return false }
            throw error
        }
        do {
            return try localFilesEqual(lhsURL, rhsURL, expectedSize: lhsMeta.size)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            return false
        }
    }

    private func localFilesEqual(_ lhsURL: URL, _ rhsURL: URL, expectedSize: Int64) throws -> Bool {
        let lhsSize = try FileManager.default.attributesOfItem(atPath: lhsURL.path)[.size] as? Int64
        let rhsSize = try FileManager.default.attributesOfItem(atPath: rhsURL.path)[.size] as? Int64
        guard lhsSize == expectedSize, rhsSize == expectedSize else { return false }

        let lhs = try FileHandle(forReadingFrom: lhsURL)
        defer { try? lhs.close() }
        let rhs = try FileHandle(forReadingFrom: rhsURL)
        defer { try? rhs.close() }

        let chunkSize = 64 * 1024
        while true {
            try Task.checkCancellation()
            let lhsChunk = try lhs.read(upToCount: chunkSize) ?? Data()
            let rhsChunk = try rhs.read(upToCount: chunkSize) ?? Data()
            if lhsChunk != rhsChunk { return false }
            if lhsChunk.isEmpty { return true }
        }
    }
}
