import Foundation
import UIKit
import os

enum MemoryDiagnostics {
    static let watermarkIntervalNanos: UInt64 = 30_000_000_000

    @MainActor
    static func watermarkLine() -> String {
        let footprint = footprintBytes().map(StageTimingWindow.formatBytes) ?? "-"
        let available = StageTimingWindow.formatBytes(Int64(os_proc_available_memory()))
        return String(
            format: String(localized: "diagnostics.log.memoryWatermark"),
            footprint,
            available,
            appStateDescription()
        )
    }

    // phys_footprint is the value jetsam accounts against, not resident size.
    static func footprintBytes() -> Int64? {
        var info = task_vm_info_data_t()
        var count = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<natural_t>.size)
        let result = withUnsafeMutablePointer(to: &info) { pointer in
            pointer.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
                task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &count)
            }
        }
        guard result == KERN_SUCCESS else { return nil }
        return Int64(clamping: info.phys_footprint)
    }

    @MainActor
    private static func appStateDescription() -> String {
        switch UIApplication.shared.applicationState {
        case .active:
            return String(localized: "diagnostics.appState.active")
        case .inactive:
            return String(localized: "diagnostics.appState.inactive")
        case .background:
            return String(localized: "diagnostics.appState.background")
        @unknown default:
            return "?"
        }
    }
}
