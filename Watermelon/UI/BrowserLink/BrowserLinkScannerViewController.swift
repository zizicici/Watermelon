import AVFoundation
import os
import SnapKit
import UIKit

private final class BrowserLinkPreviewView: UIView {
    override class var layerClass: AnyClass { AVCaptureVideoPreviewLayer.self }

    var videoPreviewLayer: AVCaptureVideoPreviewLayer {
        layer as! AVCaptureVideoPreviewLayer
    }
}

private final class BrowserLinkCaptureSessionRunner: @unchecked Sendable {
    let session = AVCaptureSession()
    private let queue = DispatchQueue(label: "com.zizicici.watermelon.link.capture", qos: .userInitiated)

    func start() {
        queue.async { [self] in
            if !session.isRunning { session.startRunning() }
        }
    }

    func stop() {
        queue.async { [self] in
            if session.isRunning { session.stopRunning() }
        }
    }
}

@MainActor
final class BrowserLinkScannerViewController: UIViewController {
    var onPairing: ((BrowserLinkPairing) -> Void)?
    var onTutorial: (() -> Void)?

    private let captureRunner = BrowserLinkCaptureSessionRunner()
    private let previewView = BrowserLinkPreviewView()
    private var lastRejectedPayload: String?
    private let frameView = UIView()
    private let instructionLabel = UILabel()
    private var didCapturePairing = false
    private var isRequestingCameraAccess = false

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "link.scanner.title")
        view.backgroundColor = .black
        navigationItem.leftBarButtonItem = UIBarButtonItem(
            barButtonSystemItem: .close,
            target: self,
            action: #selector(close)
        )
        navigationItem.rightBarButtonItem = UIBarButtonItem(
            title: String(localized: "link.tutorial.title"),
            style: .plain,
            target: self,
            action: #selector(openTutorial)
        )
        buildUI()
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(applicationDidBecomeActive),
            name: UIApplication.didBecomeActiveNotification,
            object: nil
        )
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        requestCameraAndStart()
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        captureRunner.stop()
    }

    private func buildUI() {
        previewView.backgroundColor = .black
        view.addSubview(previewView)
        previewView.snp.makeConstraints { $0.edges.equalToSuperview() }

        frameView.layer.borderWidth = 2
        frameView.layer.borderColor = UIColor.white.withAlphaComponent(0.9).cgColor
        frameView.layer.cornerRadius = 24
        frameView.isUserInteractionEnabled = false
        view.addSubview(frameView)
        frameView.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.width.equalTo(view.safeAreaLayoutGuide).multipliedBy(0.72)
            make.height.equalTo(frameView.snp.width)
        }

        instructionLabel.text = String(localized: "link.scanner.instruction")
        instructionLabel.textColor = .white
        instructionLabel.font = .preferredFont(forTextStyle: .body)
        instructionLabel.textAlignment = .center
        instructionLabel.numberOfLines = 0
        view.addSubview(instructionLabel)
        instructionLabel.snp.makeConstraints { make in
            make.top.equalTo(frameView.snp.bottom).offset(28)
            make.leading.trailing.equalTo(view.safeAreaLayoutGuide).inset(32)
        }
    }

    private func requestCameraAndStart() {
        guard viewIfLoaded?.window != nil, !didCapturePairing else { return }
        let authorization = AVCaptureDevice.authorizationStatus(for: .video)
        browserLinkLog.info("Scanner camera authorization=\(String(describing: authorization), privacy: .public)")
        switch authorization {
        case .authorized:
            configureCaptureSession()
        case .notDetermined:
            guard !isRequestingCameraAccess else { return }
            isRequestingCameraAccess = true
            AVCaptureDevice.requestAccess(for: .video) { [weak self] granted in
                Task { @MainActor [weak self, granted] in
                    self?.isRequestingCameraAccess = false
                    if granted { self?.configureCaptureSession() }
                    else { self?.showCameraUnavailable() }
                }
            }
        default:
            showCameraUnavailable()
        }
    }

    private func configureCaptureSession() {
        guard viewIfLoaded?.window != nil else { return }
        let captureSession = captureRunner.session
        if !captureSession.inputs.isEmpty {
            browserLinkLog.info("Scanner restarting configured capture session")
            captureRunner.start()
            return
        }
        guard let camera = AVCaptureDevice.default(for: .video),
              let input = try? AVCaptureDeviceInput(device: camera),
              captureSession.canAddInput(input) else {
            showCameraUnavailable()
            return
        }
        captureSession.addInput(input)
        let output = AVCaptureMetadataOutput()
        guard captureSession.canAddOutput(output) else {
            showCameraUnavailable()
            return
        }
        captureSession.addOutput(output)
        output.setMetadataObjectsDelegate(self, queue: .main)
        output.metadataObjectTypes = [.qr]

        previewView.videoPreviewLayer.session = captureSession
        previewView.videoPreviewLayer.videoGravity = .resizeAspectFill
        browserLinkLog.info("Scanner capture session configured")
        captureRunner.start()
    }

    private func accept(_ value: String) {
        guard !didCapturePairing, let url = URL(string: value) else { return }
        do {
            let pairing = try BrowserLinkPairing.parse(url)
            browserLinkLog.info("Scanner accepted pairing QR")
            didCapturePairing = true
            captureRunner.stop()
            onPairing?(pairing)
        } catch {
            browserLinkLog.error("Scanner rejected QR type=\(String(reflecting: type(of: error)), privacy: .public)")
            guard lastRejectedPayload != value else { return }
            lastRejectedPayload = value
            let generator = UINotificationFeedbackGenerator()
            generator.notificationOccurred(.error)
        }
    }

    private func showCameraUnavailable() {
        browserLinkLog.error("Scanner camera unavailable")
        guard viewIfLoaded?.window != nil, presentedViewController == nil else { return }
        let alert = UIAlertController(
            title: String(localized: "link.scanner.cameraUnavailable.title"),
            message: String(localized: "link.scanner.cameraUnavailable.message"),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "home.overlay.goToSettings"), style: .default) { _ in
            guard let url = URL(string: UIApplication.openSettingsURLString) else { return }
            UIApplication.shared.open(url)
        })
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .cancel))
        present(alert, animated: true)
    }

    @objc private func close() {
        dismiss(animated: ConsideringUser.animated)
    }

    @objc private func openTutorial() {
        captureRunner.stop()
        onTutorial?()
    }

    func resumeAfterTutorial() {
        requestCameraAndStart()
    }

    @objc private func applicationDidBecomeActive() {
        requestCameraAndStart()
    }
}

extension BrowserLinkScannerViewController: AVCaptureMetadataOutputObjectsDelegate {
    nonisolated func metadataOutput(
        _ output: AVCaptureMetadataOutput,
        didOutput metadataObjects: [AVMetadataObject],
        from connection: AVCaptureConnection
    ) {
        guard let value = (metadataObjects.first as? AVMetadataMachineReadableCodeObject)?.stringValue else { return }
        Task { @MainActor [weak self] in self?.accept(value) }
    }
}
