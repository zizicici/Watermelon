import AppKit
import SwiftUI

@main
struct WatermelonMacApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var container = MacDependencyContainer()

    var body: some Scene {
        WindowGroup {
            RootView()
                .environmentObject(container)
                .frame(minWidth: 720, minHeight: 480)
        }
        .windowStyle(.titleBar)
        .commands {
            CommandGroup(replacing: .newItem) {}
            CommandGroup(replacing: .help) {}
            CommandGroup(replacing: .pasteboard) {}
            CommandGroup(replacing: .undoRedo) {}
            CommandGroup(after: .appInfo) {
                Button(String(localized: "menu.about")) {
                    NSApplication.shared.orderFrontStandardAboutPanel(nil)
                }
            }
            CommandGroup(after: .toolbar) {
                Button(String(localized: "menu.openLogs")) {
                    let dir = ExecutionLogFileStore.directory(for: .manual)
                    try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
                    NSWorkspace.shared.open(dir)
                }
                .keyboardShortcut("L", modifiers: [.command, .shift])

                Button(String(localized: "menu.clearPerceptualCache")) {
                    Self.confirmClearPerceptualCache()
                }
            }
        }
    }

    private static func confirmClearPerceptualCache() {
        let count = PerceptualHashCache.shared.count()
        let size = PerceptualHashCache.shared.dbSize()
        let alert = NSAlert()
        alert.messageText = String(localized: "menu.clearPerceptualCache.confirm.title")
        alert.informativeText = String(
            format: String(localized: "menu.clearPerceptualCache.confirm.message"),
            count,
            ByteCountFormatter.fileSizeString(size)
        )
        alert.alertStyle = .warning
        alert.addButton(withTitle: String(localized: "menu.clearPerceptualCache.confirm.proceed"))
        alert.addButton(withTitle: String(localized: "common.cancel"))
        if alert.runModal() == .alertFirstButtonReturn {
            _ = PerceptualHashCache.shared.clearAll()
        }
    }
}
