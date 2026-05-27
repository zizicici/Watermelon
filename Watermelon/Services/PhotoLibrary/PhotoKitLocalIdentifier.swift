import Photos

// PhotoKit asset id. Local-device cache key only — stale on PhotoKit library
// reset, restore, or cross-device transfer; never a repo identity. Lives in
// the iOS target so it cannot enter Shared/ and become a wire-format key.
struct PhotoKitLocalIdentifier: RawRepresentable, Hashable, Sendable,
                                 ExpressibleByStringLiteral, CustomStringConvertible {
    let rawValue: String

    init(rawValue: String) {
        self.rawValue = rawValue
    }

    init(stringLiteral value: String) {
        self.rawValue = value
    }

    init(_ phAsset: PHAsset) {
        self.rawValue = phAsset.localIdentifier
    }

    var description: String { rawValue }
}

extension Set where Element == PhotoKitLocalIdentifier {
    var rawValues: [String] { map(\.rawValue) }
}

extension Array where Element == PhotoKitLocalIdentifier {
    var rawValues: [String] { map(\.rawValue) }
}
