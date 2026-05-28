//
//  SceneDelegate.swift
//  Watermelon
//
//  Created by Ci Zi on 2026/2/25.
//

import UIKit

class SceneDelegate: UIResponder, UIWindowSceneDelegate {

    var window: UIWindow?
    private var appCoordinator: AppCoordinator?


    func scene(_ scene: UIScene, willConnectTo session: UISceneSession, options connectionOptions: UIScene.ConnectionOptions) {
        guard let windowScene = (scene as? UIWindowScene) else { return }

        let window = UIWindow(windowScene: windowScene)
        window.tintColor = .appTint
        let coordinator = AppCoordinator(window: window)
        coordinator.start()

        self.window = window
        self.appCoordinator = coordinator
    }

    func sceneDidDisconnect(_ scene: UIScene) {
        // Trigger explicit teardown of any in-flight foreground execution / manual verify so the
        // process-wide AppRuntimeFlags lease is released before the scene's container is reclaimed.
        // Without this, the executionTask's `guard let self` retain keeps the HomeExecutionCoordinator
        // alive past scene teardown — `HomeExecutionCoordinator.deinit` would never fire and the
        // lease would leak for the rest of the process lifetime, locking out the next reconnect's
        // foreground tap, manual verify, and any scheduled BGProcessingTask.
        appCoordinator?.handleSceneDisconnect()
    }

    func sceneDidBecomeActive(_ scene: UIScene) {
        // Called when the scene has moved from an inactive state to an active state.
        // Use this method to restart any tasks that were paused (or not yet started) when the scene was inactive.
    }

    func sceneWillResignActive(_ scene: UIScene) {
        // Called when the scene will move from an active state to an inactive state.
        // This may occur due to temporary interruptions (ex. an incoming phone call).
    }

    func sceneWillEnterForeground(_ scene: UIScene) {
        AppDelegate.cancelPendingBackgroundBackup()
    }

    func sceneDidEnterBackground(_ scene: UIScene) {
        AppDelegate.scheduleNextBackgroundBackup()
    }


}
