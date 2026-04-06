import Foundation

extension Date {
    var nanosecondsSinceEpoch: Int64 {
        Int64((timeIntervalSince1970 * 1_000_000_000).rounded())
    }

    init(nanosecondsSinceEpoch ns: Int64) {
        self.init(timeIntervalSince1970: Double(ns) / 1_000_000_000)
    }
}
