import UIKit

enum MediaPresenceStyle {
    // `remoteSymbol` is the connected storage type's glyph (smb→server.rack, s3→cloud, …), matching the
    // node menu; a remote-only item is badged with it.
    static func symbolName(for presence: MediaPresence, remoteSymbol: String) -> String {
        switch presence {
        case .localOnly: return "iphone"
        case .remoteOnly: return remoteSymbol
        case .both: return "arrow.trianglehead.2.clockwise.rotate.90"
        }
    }
}
