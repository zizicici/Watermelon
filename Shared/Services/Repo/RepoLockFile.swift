import Foundation

// Structured Lite write-lock body (Repo V2 Stage B, Phase 2). Replaces the empty mtime-only marker so a
// lock carries identity plus a body timestamp for mtime-missing backends.
struct LockFileBody: Codable, Equatable, Sendable {
    let writerID: String
    let sessionToken: String
    let lockToken: String
    let generation: Int
    let writtenAt: Date?

    init(
        writerID: String,
        sessionToken: String,
        lockToken: String,
        generation: Int,
        writtenAt: Date? = nil
    ) {
        self.writerID = writerID
        self.sessionToken = sessionToken
        self.lockToken = lockToken
        self.generation = generation
        self.writtenAt = writtenAt
    }
}

enum LockFileCodec {
    static func encode(_ body: LockFileBody) throws -> Data {
        try JSONEncoder().encode(body)
    }

    // nil for empty/undecodable content; callers combine this with mtime to classify freshness/invalidity.
    static func decode(_ data: Data) -> LockFileBody? {
        guard !data.isEmpty else { return nil }
        return try? JSONDecoder().decode(LockFileBody.self, from: data)
    }
}

// One-shot read of a remote lock's identity (body) plus its freshness source (backend mtime). Used by
// the lock state machine and by metadata cleanup to second-confirm a candidate before any destructive
// action. notFound at either step collapses to `.absent`; any other transport error fails closed.
enum RemoteLockReader {
    struct Snapshot: Equatable, Sendable {
        let rawData: Data
        let body: LockFileBody?
        let modificationDate: Date?
    }

    enum State: Equatable {
        case absent
        case fault(RemoteFaultLite.Category)
        case present(Snapshot)
    }

    static func downloadBody(client: any RemoteStorageClientProtocol, path: String) async throws -> LockFileBody? {
        let temporaryURL = temporaryLockURL()
        defer { try? FileManager.default.removeItem(at: temporaryURL) }
        try await client.download(remotePath: path, localURL: temporaryURL)
        let data = (try? Data(contentsOf: temporaryURL)) ?? Data()
        return LockFileCodec.decode(data)
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

        let temporaryURL = temporaryLockURL()
        defer { try? FileManager.default.removeItem(at: temporaryURL) }
        do {
            try await client.download(remotePath: path, localURL: temporaryURL)
        } catch {
            let category = RemoteFaultLite.classify(error)
            return category == .notFound ? .absent : .fault(category)
        }
        let data: Data
        do {
            data = try Data(contentsOf: temporaryURL)
        } catch {
            return .fault(.retryable)
        }
        return .present(Snapshot(
            rawData: data,
            body: LockFileCodec.decode(data),
            modificationDate: entry.modificationDate
        ))
    }

    private static func temporaryLockURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.lockFileExtension)
    }
}
