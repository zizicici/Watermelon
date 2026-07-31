import Foundation

struct BrowserLinkNodeScopes: Codable, Sendable {
    let current: String
    let reclaim: [String]
}

enum BrowserLinkNodeScopeCodec {
    static func canonicalNodeID(_ value: String?) -> String? {
        guard let value,
              value.count == 43,
              let data = Data(base64URLEncoded: value),
              data.count == 32,
              data.base64URLEncodedString() == value else {
            return nil
        }
        return value
    }
}

extension ServerProfileRecord {
    var browserLinkFreshTakeoverScopes: Set<String> {
        browserLinkNodeScopes?.reclaim
            .reduce(into: Set<String>()) { $0.insert($1) } ?? []
    }

    var browserLinkCurrentLockScope: String? {
        browserLinkNodeScopes?.current
    }

    private var browserLinkNodeScopes: BrowserLinkNodeScopes? {
        guard isBrowserLinkProfile,
              let connectionParams,
              let scopes = try? JSONDecoder().decode(
                BrowserLinkNodeScopes.self,
                from: connectionParams
              ),
              BrowserLinkNodeScopeCodec.canonicalNodeID(
                scopes.current
              ) != nil,
              scopes.reclaim.count <= 16,
              Set(scopes.reclaim).count == scopes.reclaim.count,
              scopes.reclaim.allSatisfy({
                  BrowserLinkNodeScopeCodec.canonicalNodeID($0) != nil
              }) else {
            return nil
        }
        return scopes
    }
}
