import Foundation

struct AssetBatchFlushCounter: Sendable {
    private var count: Int = 0
    let threshold: Int

    init(threshold: Int = BackupV2Constants.batchFlushInterval) {
        self.threshold = threshold
    }

    mutating func recordSuccessAndCheckThreshold() -> Bool {
        count += 1
        return count >= threshold
    }

    mutating func reset() {
        count = 0
    }
}
