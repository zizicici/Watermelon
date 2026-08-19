import Foundation

struct InboxTransferFile: Sendable {
    let id: UUID
    let localURL: URL
    let preferredName: String
    let fileSize: Int64?
    let modificationDate: Date?
    let contentTypeIdentifier: String?
}

enum InboxTransferItem: Sendable {
    case photoAsset(localIdentifier: String)
    case file(InboxTransferFile)
}
