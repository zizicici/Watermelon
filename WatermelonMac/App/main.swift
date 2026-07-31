import AppKit

MainActor.assumeIsolated {
    let application = NSApplication.shared
    application.setActivationPolicy(.regular)
    let applicationDelegate = AppDelegate()
    application.delegate = applicationDelegate
    withExtendedLifetime(applicationDelegate) {
        application.finishLaunching()
        applicationDelegate.start()
        application.run()
    }
}
