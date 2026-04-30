import UIKit

@MainActor
final class FocusModeViewController: UIViewController {

    private let coordinator: HomeExecutionCoordinator
    private var stateObserverID: UUID?
    private var savedBrightness: CGFloat?
    private var didDisableIdleTimer = false

    init(coordinator: HomeExecutionCoordinator) {
        self.coordinator = coordinator
        super.init(nibName: nil, bundle: nil)
        modalPresentationStyle = .overFullScreen
        modalTransitionStyle = .crossDissolve
        modalPresentationCapturesStatusBarAppearance = true
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        NotificationCenter.default.removeObserver(self)
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .black

        let tap = UITapGestureRecognizer(target: self, action: #selector(handleTap))
        view.addGestureRecognizer(tap)

        NotificationCenter.default.addObserver(
            self,
            selector: #selector(appWillResignActive),
            name: UIApplication.willResignActiveNotification,
            object: nil
        )
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(appDidBecomeActive),
            name: UIApplication.didBecomeActiveNotification,
            object: nil
        )
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        enterFocusState()
        stateObserverID = coordinator.addStateObserver { [weak self] in
            self?.handleCoordinatorStateChange()
        }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if let id = stateObserverID {
            coordinator.removeStateObserver(id)
            stateObserverID = nil
        }
        exitFocusState()
    }

    override var prefersStatusBarHidden: Bool { true }
    override var prefersHomeIndicatorAutoHidden: Bool { true }

    @objc private func handleTap() {
        dismiss(animated: true)
    }

    @objc private func appWillResignActive() {
        restoreBrightness()
    }

    @objc private func appDidBecomeActive() {
        guard view.window != nil else { return }
        dimBrightness()
    }

    private func handleCoordinatorStateChange() {
        guard !coordinator.isRunning else { return }
        if didDisableIdleTimer {
            UIApplication.shared.isIdleTimerDisabled = false
            didDisableIdleTimer = false
        }
    }

    private func enterFocusState() {
        dimBrightness()
        UIApplication.shared.isIdleTimerDisabled = true
        didDisableIdleTimer = true
    }

    private func exitFocusState() {
        restoreBrightness()
        if didDisableIdleTimer {
            UIApplication.shared.isIdleTimerDisabled = false
            didDisableIdleTimer = false
        }
    }

    private func dimBrightness() {
        if savedBrightness == nil {
            savedBrightness = UIScreen.main.brightness
        }
        UIScreen.main.brightness = 0.0
    }

    private func restoreBrightness() {
        guard let saved = savedBrightness else { return }
        UIScreen.main.brightness = saved
    }
}
