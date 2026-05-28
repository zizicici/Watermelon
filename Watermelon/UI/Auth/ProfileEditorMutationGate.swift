import Foundation

/// Mutation gate shared by every storage editor's commit path. Returns true while the shared
/// execution lease or a remote verify is active — both states can hold a V2 runtime over the
/// edited profile's credentials/`repo_state`, and a save underneath that runtime would either
/// race the V2 runtime's reads or strand the keychain entry the runtime is still using.
///
/// `isVerifying` is read from BOTH the local controller (covers the common in-scene case) AND
/// the process-wide `AppRuntimeFlags.shared` flag (covers a verify task that survived a scene
/// disconnect/reconnect — the new container's controller phase is idle, but the old verify task
/// still owns the shared verify lease).
@MainActor
enum ProfileEditorMutationGate {
    /// Thrown when a commit method discovers the gate is blocked at the point an existing
    /// row would be mutated. The alert path surfaces `errorDescription` as the message body.
    struct Blocked: LocalizedError {
        var errorDescription: String? { String(localized: "home.alert.maintenanceInProgress") }
    }

    static func isBlocked(dependencies: DependencyContainer) -> Bool {
        dependencies.appRuntimeFlags.isExecuting
            || dependencies.appRuntimeFlags.isVerifying
            || dependencies.remoteMaintenanceController.isVerifying
    }

    /// Throw `Blocked` if the gate is currently blocked. Used inside editor commit methods
    /// after they discover the actual mutation target (`baseProfile`), so that an Add flow
    /// that adopts an existing duplicate row is gated the same as an explicit Edit.
    static func throwIfBlocked(dependencies: DependencyContainer) throws {
        if isBlocked(dependencies: dependencies) {
            throw Blocked()
        }
    }
}
