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
import FirebaseAnalytics
import os

@main
class AppDelegate: UIResponder, UIApplicationDelegate {

    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        FirebaseApp.configure()
        Analytics.setAnalyticsCollectionEnabled(true)
        AppExitMetricsMonitor.shared.start()

        ProStatus.migrateLegacyCacheIfNeeded()
        MoreKit.configure(
            productID: ProStatus.productID,
            membershipKey: ProStatus.membershipKey
        )
        MoreKitAppearance.shared.tintColor = .appTint
        MoreKitAppearance.shared.backgroundColor = .appBackground

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

        // Must run on the main thread: Kingfisher installs its memory-sweep Timer on the configuring
        // thread's run loop, which never spins on a pool thread.
        MediaThumbnailCache.configureIfNeeded()
        Task.detached(priority: .utility) {
            ExecutionLogFileStore.prepareForBackgroundUse()
            ExecutionLogFileStore.purgeExpired()
            // Sessions that never open the media browser still need the disk cap enforced; also trims
            // caches grown unbounded before the cap existed (≤ 1.5.5).
            await MediaThumbnailCache.enforceLimit()
            await MediaThumbnailCache.purgeLegacyDefaultCacheIfNeeded()
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

        let container: DependencyContainer
        do {
            container = try DependencyContainer.makeForBackgroundTask()
        } catch {
            task.setTaskCompleted(success: false)
            return
        }

        let runner = BackgroundBackupRunner(dependencies: container)
        let completionGuard = OSAllocatedUnfairLock(initialState: false)

        let backupTask = Task {
            await runner.run()
        }

        // Complete only after the runner fully unwinds: on expiration the cancelled run still reaches its
        // Lite write-lock release, which must finish while the app retains background execution time.
        Task {
            await backupTask.value
            let isFirst = completionGuard.withLock { (done: inout Bool) -> Bool in
                guard !done else { return false }
                done = true
                return true
            }
            if isFirst { task.setTaskCompleted(success: !backupTask.isCancelled) }
        }

        task.expirationHandler = {
            backupTask.cancel()
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
