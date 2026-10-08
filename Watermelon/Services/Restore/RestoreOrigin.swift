import Foundation

struct RestoreOrigin: Sendable, Equatable {
    let assetLocalIdentifier: String
    let localFingerprint: Data
    let remoteFingerprint: Data
}

struct RestoreOriginIndex: Sendable {
    private let byAsset: [String: [RestoreOrigin]]

    init(_ origins: [RestoreOrigin] = []) {
        byAsset = Dictionary(grouping: origins, by: \.assetLocalIdentifier)
    }

    var isEmpty: Bool { byAsset.isEmpty }

    func displayFingerprint(assetID: String, localFingerprint: Data?, remoteFingerprints: Set<Data>) -> Data? {
        guard let localFingerprint else { return nil }
        if remoteFingerprints.contains(localFingerprint) { return localFingerprint }
        return self.remoteFingerprints(for: assetID, localFingerprint: localFingerprint)
            .intersection(remoteFingerprints).sorted { $0.lexicographicallyPrecedes($1) }.first ?? localFingerprint
    }

    func remoteFingerprints(for assetID: String, localFingerprint: Data) -> Set<Data> {
        Set((byAsset[assetID] ?? []).filter { $0.localFingerprint == localFingerprint }.map(\.remoteFingerprint))
    }

    func matches(remoteFingerprint: Data, assetID: String, localFingerprint: Data?) -> Bool {
        guard let localFingerprint else { return false }
        return localFingerprint == remoteFingerprint || remoteFingerprints(for: assetID, localFingerprint: localFingerprint).contains(remoteFingerprint)
    }

    func localFingerprints(matching remoteFingerprints: Set<Data>) -> Set<Data> {
        Set(byAsset.values.joined().filter { remoteFingerprints.contains($0.remoteFingerprint) }.map(\.localFingerprint))
    }

    func localCandidates() -> [Data: [String]] {
        var result: [Data: [String]] = [:]
        for origin in byAsset.values.joined() {
            result[origin.remoteFingerprint, default: []].append(origin.assetLocalIdentifier)
        }
        return result
    }
}
