import Foundation

struct BackupScopeSummary: Sendable, Equatable {
    enum Mode: Sendable, Equatable {
        case all
        case partial
        case empty
    }

    let mode: Mode
    let selectedAssetCount: Int
    let selectedEstimatedBytes: Int64?
    let totalAssetCount: Int
    let totalEstimatedBytes: Int64?
}

struct BackupScopeSelection: Sendable, Equatable {
    // nil means full selection; non-nil means scoped selection.
    let selectedAssetIDs: Set<String>?
    let selectedAssetCount: Int
    let selectedEstimatedBytes: Int64?
    let totalAssetCount: Int
    let totalEstimatedBytes: Int64?
}
