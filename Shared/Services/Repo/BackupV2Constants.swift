import Foundation

enum BackupV2Constants {
    /// Per-month worker batch flush interval — narrows V2 commit kill window from a whole
    /// month to ~10 successful asset uploads. Both foreground (BackupParallelExecutor) and
    /// background (BackgroundBackupRunner) loops must use this same value.
    static let batchFlushInterval = 10
}
