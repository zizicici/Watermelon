import XCTest
@testable import Watermelon

// The single presence derivation every source, the viewer, and upload success now share. (The index's
// refresh/profile-gating touches a real hash index + snapshot and is manually regressed.)
final class LibraryPresenceIndexTests: XCTestCase {
    func testPresenceDerivation() {
        XCTAssertEqual(MediaPresence.of(onDevice: true, onRemote: true), .both)
        XCTAssertEqual(MediaPresence.of(onDevice: true, onRemote: false), .localOnly)
        XCTAssertEqual(MediaPresence.of(onDevice: false, onRemote: true), .remoteOnly)
        // Neither side present is degenerate (such an item shouldn't exist); it resolves to remoteOnly
        // because there is no local handle to prefer.
        XCTAssertEqual(MediaPresence.of(onDevice: false, onRemote: false), .remoteOnly)
    }
}
