import Foundation

struct RetentionPeerStatus: Equatable, Sendable {
    var writerID: String
    var status: LivenessTracker.PeerStatus
    var capability: RetentionPeerCapability?
}

struct RetentionPeerStatusView: Equatable, Sendable {
    var peers: [RetentionPeerStatus]
    var listComplete: Bool

    static let empty = RetentionPeerStatusView(peers: [], listComplete: true)

    var isComplete: Bool {
        listComplete && !peers.contains { peer in
            if case .unknown = peer.status { return true }
            return false
        }
    }
}

enum RetentionDeletionSafetyBlocker: Equatable, Sendable {
    case incompleteView
    case unknownPeer(writerID: String)
    case activePeer(writerID: String)
    case stalePeerWithinRetentionThreshold(writerID: String, lastSeenMs: Int64)
    case legacyPeerWithinGrace(writerID: String, lastSeenMs: Int64)
}

struct RetentionDeletionSafetyDecision: Equatable, Sendable {
    var blockers: [RetentionDeletionSafetyBlocker]
    var evaluatedAtMs: Int64

    var allowed: Bool { blockers.isEmpty }
}

enum RetentionDeletionSafetyGate {
    static func evaluate(
        peerStatusView: RetentionPeerStatusView,
        policy: RepoCompactionPolicy,
        manifestGate: RetentionLivenessGate,
        nowMs: Int64,
        isLocalVolume: Bool
    ) -> RetentionDeletionSafetyDecision {
        guard !isLocalVolume else {
            return RetentionDeletionSafetyDecision(blockers: [], evaluatedAtMs: nowMs)
        }

        let retentionThresholdMs = Int64(policy.retentionStalenessThresholdSeconds) * 1000
        let legacyGraceMs = max(
            Int64(policy.legacyClientGraceSeconds) * 1000,
            manifestGate.legacyClientGraceMs
        )
        var blockers: [RetentionDeletionSafetyBlocker] = []

        if manifestGate.requiredCompleteView && !peerStatusView.isComplete {
            blockers.append(.incompleteView)
        }

        for peer in peerStatusView.peers {
            switch peer.status {
            case .unknown:
                blockers.append(.unknownPeer(writerID: peer.writerID))
            case .active:
                blockers.append(.activePeer(writerID: peer.writerID))
            case .stale(let lastSeenMs):
                let ageMs = nowMs - lastSeenMs
                if peer.capability?.version == RetentionPeerCapability.currentVersion,
                   peer.capability?.barrierAwareSessionRefresh == true {
                    if ageMs < retentionThresholdMs {
                        blockers.append(.stalePeerWithinRetentionThreshold(
                            writerID: peer.writerID,
                            lastSeenMs: lastSeenMs
                        ))
                    }
                } else if ageMs < legacyGraceMs {
                    blockers.append(.legacyPeerWithinGrace(
                        writerID: peer.writerID,
                        lastSeenMs: lastSeenMs
                    ))
                }
            }
        }

        return RetentionDeletionSafetyDecision(blockers: blockers, evaluatedAtMs: nowMs)
    }
}
