import Foundation

enum RepoRetentionRuntimeDefaults {
    static let peerCapability = RetentionPeerCapability(
        barrierAwareSessionRefresh: true,
        checkpointBarrierHook: true
    )
}
