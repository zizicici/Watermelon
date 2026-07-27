import Foundation

enum RemoteSnapshotOwnership {
    static func matches(
        ownerProfileKey: String?,
        expectedProfileKey: String?
    ) -> Bool {
        ownerProfileKey == expectedProfileKey
    }
}
