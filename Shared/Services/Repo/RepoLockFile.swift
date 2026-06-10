import Foundation

// Structured Lite write-lock body (Repo V2 Stage B, Phase 2). Replaces the empty mtime-only marker so a
// lock carries identity: which writer, which session (one WriteLockService lifetime), which acquisition
// (lockToken), and a per-write generation. Token/session let a same-writer *new* session be told apart
// from an older one, and let stale takeover confirm a candidate lock has not changed before deleting it.
struct LockFileBody: Codable, Equatable, Sendable {
    let writerID: String
    let sessionToken: String
    let lockToken: String
    let generation: Int
}

enum LockFileCodec {
    static func encode(_ body: LockFileBody) throws -> Data {
        try JSONEncoder().encode(body)
    }

    // nil for empty/undecodable content (legacy empty markers, foreign formats, or a partial write):
    // callers must treat an unreadable body as "not provably mine/this-candidate" and fail closed.
    static func decode(_ data: Data) -> LockFileBody? {
        guard !data.isEmpty else { return nil }
        return try? JSONDecoder().decode(LockFileBody.self, from: data)
    }
}

// One-shot read of a remote lock's identity (body) plus its freshness source (backend mtime). Used by
// the lock state machine and by metadata cleanup to second-confirm a candidate before any destructive
// action. notFound at either step collapses to `.absent`; any other transport error fails closed.
enum RemoteLockReader {
    enum State: Equatable {
        case absent
        case fault(RemoteFaultLite.Category)
        case present(body: LockFileBody?, modificationDate: Date?)
    }

    static func read(client: any RemoteStorageClientProtocol, path: String) async -> State {
        let entry: RemoteStorageEntry?
        do {
            entry = try await client.metadata(path: path)
        } catch {
            let category = RemoteFaultLite.classify(error)
            return category == .notFound ? .absent : .fault(category)
        }
        guard let entry else { return .absent }

        let temporaryURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.lockFileExtension)
        defer { try? FileManager.default.removeItem(at: temporaryURL) }
        do {
            try await client.download(remotePath: path, localURL: temporaryURL)
        } catch {
            let category = RemoteFaultLite.classify(error)
            return category == .notFound ? .absent : .fault(category)
        }
        let data = (try? Data(contentsOf: temporaryURL)) ?? Data()
        return .present(body: LockFileCodec.decode(data), modificationDate: entry.modificationDate)
    }
}
