import Foundation

extension Date {
    var millisecondsSinceEpoch: Int64 {
        let value = (timeIntervalSince1970 * 1_000).rounded(.towardZero)
        if let ms = Int64(exactly: value) { return ms }
        if value.isNaN { return 0 }
        return value > 0 ? Int64.max : Int64.min
    }

    init(millisecondsSinceEpoch ms: Int64) {
        self.init(timeIntervalSince1970: Double(ms) / 1_000)
    }
}
