import Foundation

struct LivenessHeartbeat: Equatable, Sendable {
    var timestampMs: Int64

    init(timestampMs: Int64) {
        self.timestampMs = timestampMs
    }

    func encode() throws -> Data {
        let body: [String: Any] = ["ts": timestampMs]
        return try JSONSerialization.data(withJSONObject: body)
    }

    static func decode(_ data: Data) throws -> LivenessHeartbeat {
        guard let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let ts = strictInt64(dict["ts"]),
              ts >= 0 else {
            throw NSError(domain: "LivenessHeartbeat", code: -1, userInfo: [
                NSLocalizedDescriptionKey: "heartbeat is unreadable"
            ])
        }
        return LivenessHeartbeat(timestampMs: ts)
    }

    private static func strictInt64(_ raw: Any?) -> Int64? {
        guard let raw else { return nil }
        if CFGetTypeID(raw as CFTypeRef) == CFBooleanGetTypeID() { return nil }
        if let v = raw as? Int64 { return v }
        if let v = raw as? Int { return Int64(v) }
        return nil
    }
}
