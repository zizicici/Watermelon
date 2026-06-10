import Foundation

// Compatibility boundary for legacy V1 writers (the WatermelonMac migration import). Before any V1
// manifest write, classify the target with RepoFormatRouter and permit only a clearly-V1 or clearly-fresh
// tree. A committed Lite repo, or an unsupported/damaged/malformed Lite control state, is rejected so a
// legacy importer never writes V1 metadata over a V2 tree. A probe fault fails closed rather than guessing
// the target is safe to overwrite.
nonisolated enum LegacyV1WriteGate {
    enum Rejection: LocalizedError, Equatable {
        case committedLite           // a committed Lite/foreign version.json
        case unsupportedControlTree  // future/foreign committed format, layout mismatch, or dev marker dirs
        case damagedControlTree      // Lite month data or a malformed version with no committed version
        case probeFault(RemoteFaultLite.Category)

        var errorDescription: String? {
            switch self {
            case .committedLite:
                return "Target is already a Watermelon V2 repository; legacy V1 import is not allowed."
            case .unsupportedControlTree:
                return "Target uses an unsupported Watermelon control format; legacy V1 import is not allowed."
            case .damagedControlTree:
                return "Target has a damaged Watermelon control tree; legacy V1 import is not allowed."
            case .probeFault:
                return "Could not verify the target format; legacy V1 import was stopped."
            }
        }
    }

    // Throws a Rejection when the target is not a clearly-V1 or clearly-fresh tree.
    static func ensureWritable(client: any RemoteStorageClientProtocol, basePath: String) async throws {
        let decision: RepoFormatDecision
        do {
            decision = try await RepoFormatRouter(client: client, basePath: basePath).classify()
        } catch let RepoFormatRouterError.probeFault(category) {
            throw Rejection.probeFault(category)
        }
        switch decision {
        case .fresh, .v1Migrate:
            return                          // clearly fresh or clearly V1: permitted
        case .current:
            throw Rejection.committedLite
        case .unsupported:
            throw Rejection.unsupportedControlTree
        case .damaged, .malformedVersion:
            throw Rejection.damagedControlTree
        }
    }
}
