import Foundation

extension Notification.Name {
    static let ExecutionLifecycleDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.execution.lifecycle.changed")
    static let RemoteMaintenanceDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.remote.maintenance.changed")
    // Posted by the background runner (separate container) after a run marker lands so an active foreground
    // Home re-runs pickup; without it a marker written after the one activation pickup is missed until the
    // next lifecycle edge, which may not occur while the user keeps Home open.
    static let BackgroundBackupRunMarkerDidChange = Notification.Name(rawValue: "com.zizicici.watermelon.background.runMarker.changed")
}
