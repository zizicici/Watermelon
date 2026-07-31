import Foundation

extension Date {
    nonisolated var millisecondsSinceEpoch: Int64 {
        let value = (timeIntervalSince1970 * 1_000).rounded(.down)
        if let ms = Int64(exactly: value) { return ms }
        if value.isNaN { return 0 }
        return value > 0 ? Int64.max : Int64.min
    }

    nonisolated init(millisecondsSinceEpoch ms: Int64) {
        self.init(timeIntervalSince1970: Double(ms) / 1_000)
    }
}
