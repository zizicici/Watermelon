import Foundation

extension Notification.Name {
    static let ExecutionLifecycleDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.execution.lifecycle.changed")
    static let ShortcutSessionDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.shortcut.session.changed")
    static let ProfileMutationLeaseDidEnd = Notification.Name(rawValue: "com.zizicici.watermelon.profile.mutation.ended")
    static let ConnectionLifecycleDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.connection.lifecycle.changed")
    static let LocalIndexBuildStateDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.localIndexBuild.state.changed")
    static let RemoteMaintenanceDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.remote.maintenance.changed")
    // Posted by the background runner (separate container) after a run marker lands so an active foreground
    // Home re-runs pickup; without it a marker written after the one activation pickup is missed until the
    // next lifecycle edge, which may not occur while the user keeps Home open.
    static let BackgroundBackupRunMarkerDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.background.runMarker.changed")
    // Posted when LibraryPresenceIndex facts become stale or a requested rebuild commits.
    static let LibraryPresenceDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.library.presence.changed")
    // Posted when the browser's fingerprint-keyed L1 thumbnail cache gets a fresh image, so visible tiles that
    // previously showed the load affordance can swap in the thumbnail without a full browser reload.
    static let MediaBrowserThumbnailDidStore = Notification.Name(rawValue: "com.zizicici.watermelon.mediaBrowser.thumbnail.stored")
}
