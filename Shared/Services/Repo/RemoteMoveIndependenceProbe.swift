import Foundation

// One-time runtime check of whether a backend's MOVE is independent. Some cloud WebDAV gateways (123pan) alias
// content on MOVE: the moved-from source and the destination share one blob, so deleting the source destroys the
// destination. Publishers use this to skip temp→MOVE→delete on such backends. GET is authoritative — PROPFIND can
// still list a destroyed file on these gateways — so survival is checked by download, not exists.
nonisolated enum RemoteMoveIndependenceProbe {
    // True when MOVE is NOT independent (deleting the moved-from source loses the destination). Fail-safe: any
    // fault or ambiguity returns true, so an uncertain backend takes the safe direct-publish path.
    static func detectNonIndependentMove(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async -> Bool {
        let repoDir = RepoLayoutLite.repoDirectoryPath(basePath: basePath)
        let token = UUID().uuidString
        let sourcePath = RepoLayoutLite.moveProbeScratchPath(basePath: basePath, token: token, suffix: "src")
        let destPath = RepoLayoutLite.moveProbeScratchPath(basePath: basePath, token: token, suffix: "dst")
        let probeBytes = Data(token.utf8)
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent("movecheck-\(token)")
        let verifyURL = FileManager.default.temporaryDirectory.appendingPathComponent("movecheck-\(token)-verify")
        defer {
            try? FileManager.default.removeItem(at: localURL)
            try? FileManager.default.removeItem(at: verifyURL)
        }

        func cleanup() async {
            try? await client.delete(path: sourcePath)
            try? await client.delete(path: destPath)
        }

        do {
            try probeBytes.write(to: localURL)
            try await client.createDirectory(path: repoDir)
            try await client.upload(localURL: localURL, remotePath: sourcePath, respectTaskCancellation: false, onProgress: nil)
            try await client.move(from: sourcePath, to: destPath)
            // Delete the moved-from source. Only a CONFIRMED not-found is ignorable (an independent MOVE already
            // removed it); any other delete fault means the source may still alias the destination, so the survival
            // check below would be meaningless — fail safe to non-independent.
            do {
                try await client.delete(path: sourcePath)
            } catch {
                guard RemoteFaultLite.classify(error) == .notFound else {
                    await cleanup()
                    return true
                }
            }
            try await client.download(remotePath: destPath, localURL: verifyURL)
            let survived = (try? Data(contentsOf: verifyURL)) == probeBytes
            await cleanup()
            return !survived
        } catch {
            await cleanup()
            return true
        }
    }
}
