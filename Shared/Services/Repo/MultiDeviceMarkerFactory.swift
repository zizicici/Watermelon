import Foundation

enum MultiDeviceMarkerFactory {
    // Diagnostic multi-device marker for a profile's Lite acquire. nil when the profile is unsaved.
    static func make(
        for profile: ServerProfileRecord,
        databaseManager: DatabaseManager
    ) -> (@Sendable () async -> Void)? {
        guard let profileID = profile.id else { return nil }
        return { try? databaseManager.setMultiDeviceObserved(Date(), profileID: profileID) }
    }
}
