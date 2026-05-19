import Foundation

struct LivenessHeartbeat: Equatable, Sendable {
    var timestampMs: Int64
    var retention: RetentionPeerCapability?

    init(timestampMs: Int64, retention: RetentionPeerCapability?) {
        self.timestampMs = timestampMs
        self.retention = retention
    }

    func encode() throws -> Data {
        var body: [String: Any] = ["ts": timestampMs]
        if let retention {
            body["retention"] = [
                "version": retention.version,
                "barrier_aware_session_refresh": retention.barrierAwareSessionRefresh,
                "checkpoint_barrier_hook": retention.checkpointBarrierHook
            ]
        }
        return try JSONSerialization.data(withJSONObject: body)
    }

    static func decode(_ data: Data) throws -> LivenessHeartbeat {
        guard let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let ts = strictInt64(dict["ts"]) else {
            throw NSError(domain: "LivenessHeartbeat", code: -1, userInfo: [
                NSLocalizedDescriptionKey: "heartbeat is unreadable"
            ])
        }
        return LivenessHeartbeat(
            timestampMs: ts,
            retention: decodeRetention(dict["retention"])
        )
    }

    private static func decodeRetention(_ raw: Any?) -> RetentionPeerCapability? {
        guard let raw, !(raw is NSNull),
              let dict = raw as? [String: Any],
              let version = strictInt(dict["version"]),
              version == RetentionPeerCapability.currentVersion,
              let barrierAware = strictBool(dict["barrier_aware_session_refresh"]),
              let checkpointHook = strictBool(dict["checkpoint_barrier_hook"]) else {
            return nil
        }
        return RetentionPeerCapability(
            version: version,
            barrierAwareSessionRefresh: barrierAware,
            checkpointBarrierHook: checkpointHook
        )
    }

    private static func strictInt(_ raw: Any?) -> Int? {
        guard let raw else { return nil }
        if CFGetTypeID(raw as CFTypeRef) == CFBooleanGetTypeID() { return nil }
        return raw as? Int
    }

    private static func strictInt64(_ raw: Any?) -> Int64? {
        guard let raw else { return nil }
        if CFGetTypeID(raw as CFTypeRef) == CFBooleanGetTypeID() { return nil }
        if let v = raw as? Int64 { return v }
        if let v = raw as? Int { return Int64(v) }
        return nil
    }

    private static func strictBool(_ raw: Any?) -> Bool? {
        guard let raw else { return nil }
        if CFGetTypeID(raw as CFTypeRef) != CFBooleanGetTypeID() { return nil }
        return raw as? Bool
    }
}
