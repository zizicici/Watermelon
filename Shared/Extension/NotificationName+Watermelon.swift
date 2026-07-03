import Foundation

extension Notification.Name {
    static let ExecutionLifecycleDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.execution.lifecycle.changed")
    static let RemoteMaintenanceDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.remote.maintenance.changed")
    // Posted by the background runner (separate container) after a run marker lands so an active foreground
    // Home re-runs pickup; without it a marker written after the one activation pickup is missed until the
    // next lifecycle edge, which may not occur while the user keeps Home open.
    static let BackgroundBackupRunMarkerDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.background.runMarker.changed")
    // Posted by LibraryPresenceIndex after a rebuild so open browser UI can re-derive local/remote/both.
    static let LibraryPresenceDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.library.presence.changed")
    // Posted by RemoteIndexSyncService once a sync commits changes to the cached snapshot — the single
    // authoritative "the browser-visible remote library changed" signal. Fires AFTER the cache is updated, so
    // a presence rebuild off it can't read a pre-reload snapshot (closes the background-reload race). Coarse:
    // one post per changed sync, never per uploaded asset (the upload hot path does not post).
    static let RemoteLibrarySnapshotDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.remote.library.snapshot.changed")
}
