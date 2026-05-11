import CryptoKit
import Foundation

/// Hash = `sha256(line1\nline2\n...\nlineN)` — NO trailing `\n`. Writers append a trailing
/// `\n` for cleanliness; readers strip it before feeding `absorbLine`. Both sides must agree.
struct IntegrityAccumulator {
    private var hasher = SHA256()
    private(set) var rowCount: Int = 0
    private var hasAccumulated = false

    mutating func absorbLine(_ raw: String) {
        if hasAccumulated {
            hasher.update(data: Data([0x0a]))
        }
        if let utf8 = raw.data(using: .utf8) {
            hasher.update(data: utf8)
        }
        hasAccumulated = true
        rowCount += 1
    }

    func finalize() -> String {
        let copy = hasher
        let digest = copy.finalize()
        return Data(digest).hexString
    }
}

enum IntegrityResult: Equatable, Sendable {
    case ok
    case mismatchedSha256(expected: String, actual: String)
    case mismatchedRowCount(expected: Int, actual: Int)
}

func verifyIntegrity(expectedSha256: String, expectedRowCount: Int, actualSha256: String, actualRowCount: Int) -> IntegrityResult {
    if expectedSha256.lowercased() != actualSha256.lowercased() {
        return .mismatchedSha256(expected: expectedSha256, actual: actualSha256)
    }
    if expectedRowCount != actualRowCount {
        return .mismatchedRowCount(expected: expectedRowCount, actual: actualRowCount)
    }
    return .ok
}
