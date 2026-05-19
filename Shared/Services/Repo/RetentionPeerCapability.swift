import Foundation

struct RetentionPeerCapability: Equatable, Sendable {
    static let currentVersion = 1

    var version: Int
    var barrierAwareSessionRefresh: Bool
    var checkpointBarrierHook: Bool

    init(
        version: Int = RetentionPeerCapability.currentVersion,
        barrierAwareSessionRefresh: Bool,
        checkpointBarrierHook: Bool
    ) {
        self.version = version
        self.barrierAwareSessionRefresh = barrierAwareSessionRefresh
        self.checkpointBarrierHook = checkpointBarrierHook
    }
}
