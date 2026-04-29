import Foundation

struct BackupScopeSelection: Sendable, Equatable {
    // nil means full selection; non-nil means scoped selection.
    let selectedAssetIDs: Set<String>?
    let selectedAssetCount: Int
    let selectedEstimatedBytes: Int64?
    let totalAssetCount: Int
    let totalEstimatedBytes: Int64?
}
