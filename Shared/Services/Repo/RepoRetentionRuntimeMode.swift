import Foundation

struct RepoRetentionRuntimeMode: Sendable, Equatable {
    var barrierAwareSessionRefresh: Bool

    static let disabled = RepoRetentionRuntimeMode(barrierAwareSessionRefresh: false)
    static let barrierAwareSessionRefreshOnly = RepoRetentionRuntimeMode(barrierAwareSessionRefresh: true)
}
