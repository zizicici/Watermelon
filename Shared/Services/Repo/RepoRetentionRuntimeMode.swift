import Foundation

struct RepoRetentionRuntimeMode: Sendable, Equatable {
    let barrierAwareSessionRefresh: Bool
    let checkpointBarrierHook: Bool
    let compactionPolicy: RepoCompactionPolicy

    private init(
        barrierAwareSessionRefresh: Bool,
        checkpointBarrierHook: Bool,
        compactionPolicy: RepoCompactionPolicy
    ) {
        self.barrierAwareSessionRefresh = barrierAwareSessionRefresh
        self.checkpointBarrierHook = checkpointBarrierHook
        self.compactionPolicy = compactionPolicy
    }

    static let disabled = RepoRetentionRuntimeMode(
        barrierAwareSessionRefresh: false,
        checkpointBarrierHook: false,
        compactionPolicy: .default
    )

    static let barrierAwareSessionRefreshOnly = RepoRetentionRuntimeMode(
        barrierAwareSessionRefresh: true,
        checkpointBarrierHook: false,
        compactionPolicy: .default
    )

    static func checkpointBarrierHookOnly(
        policy: RepoCompactionPolicy = .default
    ) -> RepoRetentionRuntimeMode {
        RepoRetentionRuntimeMode(
            barrierAwareSessionRefresh: true,
            checkpointBarrierHook: true,
            compactionPolicy: policy
        )
    }

    var retentionPeerCapability: RetentionPeerCapability? {
        guard barrierAwareSessionRefresh else { return nil }
        return RetentionPeerCapability(
            barrierAwareSessionRefresh: true,
            checkpointBarrierHook: checkpointBarrierHook
        )
    }
}
