//
//  AppDelegate.swift
//  Watermelon
//
//  Created by Ci Zi on 2026/2/25.
//

import UIKit
import MoreKit
import BackgroundTasks
import FirebaseCore
import os

@main
class AppDelegate: UIResponder, UIApplicationDelegate {

    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        FirebaseApp.configure()

        ProStatus.setupStoreObserver()

        MoreKit.configure(
            productID: ProStatus.productID,
            membershipKey: "com.zizicici.watermelon.membership.lifetime"
        )
        MoreKitAppearance.shared.tintColor = .systemGreen
        MoreKitAppearance.shared.backgroundColor = .appBackground

        Task { await ProStatus.verifyEntitlement() }

        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: BackgroundBackupRunner.taskIdentifier,
            using: nil
        ) { task in
            guard let processingTask = task as? BGProcessingTask else {
                task.setTaskCompleted(success: false)
                return
            }
            Self.handleBackgroundBackup(processingTask)
        }

        Task.detached(priority: .utility) {
            ExecutionLogFileStore.purgeExpired()
        }

        return true
    }

    private static func handleBackgroundBackup(_ task: BGProcessingTask) {
        if BackgroundBackupSetting.getValue() == .enable {
            let request = BGProcessingTaskRequest(identifier: BackgroundBackupRunner.taskIdentifier)
            request.requiresNetworkConnectivity = true
            request.requiresExternalPower = true
            try? BGTaskScheduler.shared.submit(request)
        }

        let container = DependencyContainer()
        let runner = BackgroundBackupRunner(dependencies: container)
        let completionGuard = OSAllocatedUnfairLock(initialState: false)

        let backupTask = Task {
            await runner.run()
            let isFirst = completionGuard.withLock { (done: inout Bool) -> Bool in
                guard !done else { return false }
                done = true
                return true
            }
            if isFirst { task.setTaskCompleted(success: true) }
        }

        task.expirationHandler = {
            backupTask.cancel()
            let isFirst = completionGuard.withLock { (done: inout Bool) -> Bool in
                guard !done else { return false }
                done = true
                return true
            }
            if isFirst { task.setTaskCompleted(success: false) }
        }
    }

    @MainActor
    static func scheduleNextBackgroundBackup() {
        guard ProStatus.isPro,
              BackgroundBackupSetting.getValue() == .enable else { return }

        let request = BGProcessingTaskRequest(
            identifier: BackgroundBackupRunner.taskIdentifier
        )
        request.requiresNetworkConnectivity = true
        request.requiresExternalPower = true
        try? BGTaskScheduler.shared.submit(request)
    }

    static func cancelPendingBackgroundBackup() {
        BGTaskScheduler.shared.cancel(
            taskRequestWithIdentifier: BackgroundBackupRunner.taskIdentifier
        )
    }

    // MARK: UISceneSession Lifecycle

    func application(_ application: UIApplication, configurationForConnecting connectingSceneSession: UISceneSession, options: UIScene.ConnectionOptions) -> UISceneConfiguration {
        return UISceneConfiguration(name: "Default Configuration", sessionRole: connectingSceneSession.role)
    }

    func application(_ application: UIApplication, didDiscardSceneSessions sceneSessions: Set<UISceneSession>) {
    }
}
