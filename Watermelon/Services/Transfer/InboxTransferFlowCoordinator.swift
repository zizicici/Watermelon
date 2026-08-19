import MoreKit
import UIKit

actor InboxTransferPauseGate {
    private var isPaused = false

    func setPaused(_ paused: Bool) {
        isPaused = paused
    }

    func waitUntilResumed(
        onPauseReached: @Sendable () async -> Void
    ) async throws -> Bool {
        var reportedPause = false
        while isPaused {
            if !reportedPause {
                reportedPause = true
                await onPauseReached()
            }
            try Task.checkCancellation()
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        return reportedPause
    }
}

@MainActor
final class InboxTransferActivity {
    let pauseGate = InboxTransferPauseGate()
    private(set) var terminalFailureStatus: String?
    private let onUpdate: (String) -> Void
    private let onPauseStateChange: (Bool) -> Void

    init(
        onUpdate: @escaping (String) -> Void,
        onPauseStateChange: @escaping (Bool) -> Void
    ) {
        self.onUpdate = onUpdate
        self.onPauseStateChange = onPauseStateChange
    }

    func update(_ status: String) {
        onUpdate(status)
    }

    func updatePauseState(_ paused: Bool) {
        onPauseStateChange(paused)
    }

    func fail(_ status: String) {
        terminalFailureStatus = status
        onUpdate(status)
    }
}

@MainActor
final class InboxTransferFlowCoordinator {
    private enum ExecutionOutcome: @unchecked Sendable {
        case success(InboxTransferResult, preparedProfile: ServerProfileRecord?)
        case failure(Error, preparedProfile: ServerProfileRecord?)
    }

    struct Hooks {
        let canChooseDestination: () -> Bool
        let openNewStorage: (
            NewStorageDestination,
            [InboxTransferItem],
            InboxTransferOptions,
            UIViewController
        ) -> Void
        let setBrowserLinkSessionActive: (Bool) -> Void
        let suppressAutoConnectForBrowserLink: () -> Void
        let adoptPreparedProfile: (ServerProfileRecord) -> Void
    }

    private let dependencies: DependencyContainer
    private let hooks: Hooks
    private var browserLinkSessionID: UUID?

    init(dependencies: DependencyContainer, hooks: Hooks) {
        self.dependencies = dependencies
        self.hooks = hooks
    }

    func handle(
        _ destination: InboxTransferDestination,
        items: [InboxTransferItem],
        presenter: UIViewController,
        options: InboxTransferOptions,
        activity: InboxTransferActivity
    ) async -> Bool {
        guard validateTransferAccess(items: items, presenter: presenter, activity: activity) else {
            return false
        }
        guard hooks.canChooseDestination() else {
            let message = String(localized: "mediaBrowser.action.taskInProgress")
            activity.fail(message)
            presentError(message, on: presenter)
            return false
        }
        switch destination {
        case .profile(let profile):
            return await performTransfer(
                items,
                profile: profile,
                from: presenter,
                options: options,
                activity: activity
            )
        case .browserLink:
            openBrowserLink(items: items, options: options, from: presenter)
            return false
        case .newStorage(let destination):
            hooks.openNewStorage(destination, items, options, presenter)
            return false
        }
    }

    func performTransfer(
        _ items: [InboxTransferItem],
        profile: ServerProfileRecord,
        credential: String? = nil,
        from presenter: UIViewController,
        options: InboxTransferOptions,
        activity: InboxTransferActivity
    ) async -> Bool {
        guard validateTransferAccess(items: items, presenter: presenter, activity: activity) else {
            return false
        }
        guard !items.isEmpty else { return false }
        let modalContainer = presenter.navigationController ?? presenter
        modalContainer.isModalInPresentation = true
        defer { modalContainer.isModalInPresentation = false }
        do {
            activity.update(String(localized: "transfer.progress.preparing"))
            let outcome = await dependencies.appRuntimeFlags.withExecutionLease {
                var profileToAdopt: ServerProfileRecord?
                do {
                    let password: String
                    if let credential {
                        password = credential
                    } else {
                        password = try await self.transferCredential(for: profile, presenter: presenter)
                    }
                    let preparedProfile = try await self.dependencies.storageProfileConnectionService.prepareForConnection(
                        profile: profile,
                        confirmSFTPHostKey: { [weak self, weak presenter] decision, actual in
                            guard let self, let presenter else { return false }
                            return await self.confirmSFTPHostKey(decision: decision, actual: actual, presenter: presenter)
                        }
                    )
                    if preparedProfile.connectionParams != profile.connectionParams {
                        profileToAdopt = preparedProfile
                    }
                    let result = try await self.dependencies.inboxTransferService.transfer(
                        items: items,
                        profile: preparedProfile,
                        password: password,
                        options: options,
                        pauseGate: activity.pauseGate,
                        onProgress: { progress in
                            activity.update(String.localizedStringWithFormat(
                                String(localized: "transfer.progress.sending"),
                                progress.completedFileCount,
                                progress.totalFileCount
                            ))
                        },
                        onPauseStateChanged: { paused in
                            activity.updatePauseState(paused)
                        }
                    )
                    return ExecutionOutcome.success(result, preparedProfile: profileToAdopt)
                } catch {
                    return ExecutionOutcome.failure(error, preparedProfile: profileToAdopt)
                }
            }
            guard let outcome else {
                let message = String(localized: "mediaBrowser.action.taskInProgress")
                activity.fail(message)
                presentError(message, on: presenter)
                return false
            }
            switch outcome {
            case .success(let result, let preparedProfile):
                if let preparedProfile {
                    hooks.adoptPreparedProfile(preparedProfile)
                }
                activity.update(String.localizedStringWithFormat(
                    String(localized: "transfer.success"),
                    result.fileCount
                ))
                return true
            case .failure(let error, let preparedProfile):
                if let preparedProfile {
                    hooks.adoptPreparedProfile(preparedProfile)
                }
                throw error
            }
        } catch {
            let failure = error as? InboxTransferFailure
            let underlying = failure?.underlying ?? error
            if underlying is CancellationError { return false }
            let completedFileCount = failure?.completedFileCount ?? 0
            let detail = profile.userFacingStorageErrorMessage(underlying)
            let message = completedFileCount > 0
                ? String.localizedStringWithFormat(
                    String(localized: "transfer.error.partial"),
                    completedFileCount,
                    detail
                )
                : detail
            activity.fail(message)
            presentError(message, on: presenter)
            return false
        }
    }

    private func transferCredential(
        for profile: ServerProfileRecord,
        presenter: UIViewController
    ) async throws -> String {
        let session = dependencies.appSession.snapshot
        if session.activeProfile?.runtimeConnectionIdentity == profile.runtimeConnectionIdentity {
            if profile.storageProfile.requiresStoredCredential {
                if let activeCredential = session.activePassword { return activeCredential }
            } else {
                return session.activePassword ?? ""
            }
        }
        guard profile.storageProfile.requiresStoredCredential else { return "" }
        if let saved = try? dependencies.keychainService.readPassword(account: profile.credentialRef) {
            return saved
        }
        guard profile.storageProfile.supportsPasswordPrompt else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        guard let password = await requestCredential(for: profile, presenter: presenter) else {
            throw CancellationError()
        }
        try dependencies.keychainService.save(password: password, account: profile.credentialRef)
        return password
    }

    private func validateTransferAccess(
        items: [InboxTransferItem],
        presenter: UIViewController,
        activity: InboxTransferActivity
    ) -> Bool {
        let policy = InboxTransferAccessPolicy(isPro: ProStatus.isPro)
        guard !policy.allows(itemCount: items.count) else { return true }
        let message = policy.buttonSubtitle ?? String(localized: "transfer.limit.free")
        activity.fail(message)
        presentError(message, on: presenter)
        return false
    }

    private func requestCredential(
        for profile: ServerProfileRecord,
        presenter: UIViewController
    ) async -> String? {
        await withCheckedContinuation { continuation in
            let title: String
            let placeholder: String
            if profile.resolvedStorageType == .s3 {
                title = String(localized: "home.alert.s3SecretKeyPrompt")
                placeholder = String(localized: "auth.s3.placeholder.secretKey")
            } else {
                title = String(localized: "home.alert.passwordPrompt")
                placeholder = String(localized: "home.alert.passwordPlaceholder")
            }
            let alert = UIAlertController(title: title, message: profile.name, preferredStyle: .alert)
            alert.addTextField { textField in
                textField.placeholder = placeholder
                textField.isSecureTextEntry = true
            }
            alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel) { _ in
                continuation.resume(returning: nil)
            })
            alert.addAction(UIAlertAction(title: String(localized: "common.connect"), style: .default) { _ in
                continuation.resume(returning: alert.textFields?.first?.text)
            })
            presenter.present(alert, animated: true)
        }
    }

    private func confirmSFTPHostKey(
        decision: SFTPHostKeyPromptPolicy.Decision,
        actual: String,
        presenter: UIViewController
    ) async -> Bool {
        let title: String
        let message: String
        let confirmTitle: String
        let confirmStyle: UIAlertAction.Style
        switch decision {
        case .none:
            return true
        case .firstTrust:
            title = String(localized: "auth.sftp.hostKey.confirmTitle")
            message = String.localizedStringWithFormat(String(localized: "auth.sftp.hostKey.confirmBody"), actual)
            confirmTitle = String(localized: "auth.sftp.hostKey.confirmAction")
            confirmStyle = .default
        case .changedKey(let expected):
            title = String(localized: "auth.sftp.hostKey.changedTitle")
            message = String.localizedStringWithFormat(
                String(localized: "auth.sftp.hostKey.changedBody"),
                expected,
                actual
            )
            confirmTitle = String(localized: "auth.sftp.hostKey.changedAction")
            confirmStyle = .destructive
        }
        return await withCheckedContinuation { continuation in
            let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel) { _ in
                continuation.resume(returning: false)
            })
            alert.addAction(UIAlertAction(title: confirmTitle, style: confirmStyle) { _ in
                continuation.resume(returning: true)
            })
            presenter.present(alert, animated: true)
        }
    }

    private func presentError(_ message: String, on presenter: UIViewController) {
        guard presenter.viewIfLoaded?.window != nil,
              presenter.presentedViewController == nil else { return }
        let alert = UIAlertController(
            title: String(localized: "common.error"),
            message: message,
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .cancel))
        presenter.present(alert, animated: true)
    }

    private func openBrowserLink(
        items: [InboxTransferItem],
        options: InboxTransferOptions,
        from presenter: UIViewController
    ) {
        guard browserLinkSessionID == nil, hooks.canChooseDestination() else {
            presentBrowserLinkError(String(localized: "link.connection.busy"), on: presenter)
            return
        }
        let sessionID = UUID()
        browserLinkSessionID = sessionID
        hooks.setBrowserLinkSessionActive(true)
        hooks.suppressAutoConnectForBrowserLink()
        guard BrowserLinkTutorialViewController.CompletionGate.hasCompleted else {
            presentBrowserLinkTutorial(
                items: items,
                options: options,
                from: presenter,
                sessionID: sessionID
            )
            return
        }
        presentBrowserLinkScanner(
            items: items,
            options: options,
            from: presenter,
            sessionID: sessionID
        )
    }

    private func presentBrowserLinkTutorial(
        items: [InboxTransferItem],
        options: InboxTransferOptions,
        from presenter: UIViewController,
        sessionID: UUID
    ) {
        let tutorial = BrowserLinkTutorialViewController(allowsDismissal: false)
        let navigation = UINavigationController(rootViewController: tutorial)
        navigation.modalPresentationStyle = .pageSheet
        navigation.isModalInPresentation = true
        if let presentation = navigation.sheetPresentationController {
            presentation.detents = [.large()]
            presentation.prefersGrabberVisible = false
        }
        tutorial.onCompleted = { [weak self, weak navigation, weak presenter] in
            BrowserLinkTutorialViewController.CompletionGate.markCompleted()
            navigation?.dismiss(animated: ConsideringUser.animated) { [weak self, weak presenter] in
                guard let self,
                      let presenter,
                      self.browserLinkSessionID == sessionID else { return }
                self.presentBrowserLinkScanner(
                    items: items,
                    options: options,
                    from: presenter,
                    sessionID: sessionID
                )
            }
        }
        presenter.present(navigation, animated: ConsideringUser.animated)
    }

    private func presentBrowserLinkScanner(
        items: [InboxTransferItem],
        options: InboxTransferOptions,
        from presenter: UIViewController,
        sessionID: UUID
    ) {
        let scanner = BrowserLinkScannerViewController()
        let container = makeBrowserLinkContainer(rootViewController: scanner, sessionID: sessionID)
        scanner.onTutorial = { [weak scanner] in
            guard let scanner else { return }
            let tutorial = BrowserLinkTutorialViewController(allowsDismissal: true)
            let navigation = UINavigationController(rootViewController: tutorial)
            if let presentation = navigation.sheetPresentationController {
                presentation.detents = [.large()]
                presentation.prefersGrabberVisible = true
            }
            tutorial.onDismissed = { [weak scanner] in scanner?.resumeAfterTutorial() }
            tutorial.onCompleted = { [weak navigation] in
                BrowserLinkTutorialViewController.CompletionGate.markCompleted()
                navigation?.dismiss(animated: ConsideringUser.animated)
            }
            scanner.present(navigation, animated: ConsideringUser.animated)
        }
        scanner.onPairing = { [weak self, weak container, weak presenter] pairing in
            guard let self, let container, let presenter else { return }
            guard self.hooks.canChooseDestination() else {
                container.dismiss(animated: ConsideringUser.animated) { [weak self, weak presenter] in
                    guard let self, let presenter else { return }
                    self.presentBrowserLinkError(String(localized: "link.connection.busy"), on: presenter)
                }
                return
            }
            container.isModalInPresentation = true
            container.pushViewController(
                self.makeBrowserLinkConnection(
                    pairing: pairing,
                    items: items,
                    options: options,
                    presenter: presenter,
                    container: container
                ),
                animated: ConsideringUser.pushAnimated
            )
        }
        if let presentation = container.sheetPresentationController {
            presentation.detents = [.large()]
            presentation.prefersGrabberVisible = true
        }
        presenter.present(container, animated: ConsideringUser.animated)
    }

    private func makeBrowserLinkConnection(
        pairing: BrowserLinkPairing,
        items: [InboxTransferItem],
        options: InboxTransferOptions,
        presenter: UIViewController,
        container: UINavigationController
    ) -> BrowserLinkConnectionViewController {
        let connection = BrowserLinkConnectionViewController(
            pairing: pairing,
            transferRateLimitBytesPerSecond: BrowserLinkTransferRatePolicy.maximumBytesPerSecond(
                rateLimitEnabled: BrowserLinkRateLimitSetting.getValue() == .standard
            )
        )
        connection.onAuthenticated = { [weak self, weak connection, weak container, weak presenter] client in
            guard let self, let connection, let container, let presenter else { return }
            let storageClient = BrowserLinkStorageClient(
                client: client,
                installsTimestampTools: false
            )
            let profile = BrowserLinkStorageClient.makeProfile(
                pairing: pairing,
                folderName: client.remoteFolderName,
                browserNodeID: client.remoteBrowserNodeID,
                reclaimBrowserNodeIDs: client.reclaimBrowserNodeIDs
            )
            let registration = self.dependencies.storageClientFactory.registerBrowserLink(
                sessionID: pairing.sessionID,
                client: storageClient
            )
            connection.markHandedOff()
            container.dismiss(animated: ConsideringUser.animated) { [weak self, weak presenter] in
                guard let self else {
                    client.stop()
                    return
                }
                guard let presenter,
                      let browser = presenter as? MediaBrowserGridViewController else {
                    self.dependencies.storageClientFactory.unregisterBrowserLink(token: registration)
                    client.stop()
                    return
                }
                let started = browser.runExternalSelectionAction { [weak self, weak presenter] activity in
                    guard let self, let presenter else { return false }
                    defer {
                        self.dependencies.storageClientFactory.unregisterBrowserLink(token: registration)
                        client.stop()
                    }
                    return await self.performTransfer(
                        items,
                        profile: profile,
                        credential: "",
                        from: presenter,
                        options: options,
                        activity: activity
                    )
                }
                if !started {
                    self.dependencies.storageClientFactory.unregisterBrowserLink(token: registration)
                    client.stop()
                }
            }
        }
        return connection
    }

    private func makeBrowserLinkContainer(
        rootViewController: UIViewController,
        sessionID: UUID
    ) -> BrowserLinkSessionNavigationController {
        let container = BrowserLinkSessionNavigationController(rootViewController: rootViewController)
        container.onSessionEnded = { [weak self] in
            guard let self, self.browserLinkSessionID == sessionID else { return }
            self.browserLinkSessionID = nil
            self.hooks.setBrowserLinkSessionActive(false)
        }
        return container
    }

    private func presentBrowserLinkError(_ message: String, on presenter: UIViewController) {
        let alert = UIAlertController(
            title: String(localized: "link.connection.unavailableTitle"),
            message: message,
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .cancel))
        presenter.present(alert, animated: true)
    }
}
