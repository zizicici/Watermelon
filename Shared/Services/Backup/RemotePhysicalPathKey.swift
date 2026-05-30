import Foundation

/// Byte-exact identity for a remote physical path. Swift `String` hashing/equality
/// fold Unicode-canonically-equivalent forms (NFC vs NFD) to one key, so an
/// exact-name backend that stores both spellings as distinct objects would have one
/// of its resource rows silently overwritten in a `[String: …]` map. This key hashes
/// and compares the raw UTF-8 bytes while retaining the original string for wire/UI/
/// remote-call emission.
struct RemotePhysicalPathKey: Hashable, Sendable {
    /// Original spelling — use for download/list/delete, wire encode, restore, UI.
    let path: String
    private let bytes: [UInt8]

    init(_ path: String) {
        self.path = path
        self.bytes = Array(path.utf8)
    }

    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.bytes == rhs.bytes
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(bytes)
    }
}
