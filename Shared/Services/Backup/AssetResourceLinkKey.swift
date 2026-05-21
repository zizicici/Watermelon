import Foundation

struct AssetResourceLinkKey: Hashable, Sendable {
    let role: Int
    let slot: Int
    let hash: Data

    init(role: Int, slot: Int, hash: Data) {
        self.role = role
        self.slot = slot
        self.hash = hash
    }

    init(_ link: RemoteAssetResourceLink) {
        self.init(role: link.role, slot: link.slot, hash: link.resourceHash)
    }
}

enum AssetResourceLinkSetPredicate {
    static func keys(fromTuples tuples: [(role: Int, slot: Int, hash: Data)]) -> Set<AssetResourceLinkKey> {
        Set(tuples.map { AssetResourceLinkKey(role: $0.role, slot: $0.slot, hash: $0.hash) })
    }

    static func keys(fromLinks links: [RemoteAssetResourceLink]) -> Set<AssetResourceLinkKey> {
        Set(links.map(AssetResourceLinkKey.init))
    }

    static func isStrictSubset(
        _ candidate: Set<AssetResourceLinkKey>,
        of incoming: Set<AssetResourceLinkKey>
    ) -> Bool {
        guard !candidate.isEmpty, candidate.count < incoming.count else { return false }
        return incoming.isSuperset(of: candidate)
    }

    static func isSuperset(
        _ candidate: Set<AssetResourceLinkKey>,
        of needed: Set<AssetResourceLinkKey>
    ) -> Bool {
        guard !needed.isEmpty else { return false }
        return candidate.isSuperset(of: needed)
    }
}
