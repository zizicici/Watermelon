import Foundation

/// Mutation gate shared by every storage editor's commit path. Returns true while the shared
/// execution lease or a remote verify is active — both states can hold a V2 runtime over the
/// edited profile's credentials/`repo_state`, and a save underneath that runtime would either
/// race the V2 runtime's reads or strand the keychain entry the runtime is still using.
@MainActor
enum ProfileEditorMutationGate {
    static func isBlocked(dependencies: DependencyContainer) -> Bool {
        dependencies.appRuntimeFlags.isExecuting || dependencies.remoteMaintenanceController.isVerifying
    }
}
