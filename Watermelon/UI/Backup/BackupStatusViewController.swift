import SnapKit
import UIKit

@MainActor
final class BackupStatusViewController: UIViewController {
    private let sessionController: BackupSessionController

    private let stateLabel = UILabel()
    private let progressLabel = UILabel()
    private let logTextView = UITextView()

    private let startButton = UIButton(type: .system)
    private let pauseButton = UIButton(type: .system)
    private let stopButton = UIButton(type: .system)

    private var observerID: UUID?
    private var renderedLogCount: Int = 0

    init(sessionController: BackupSessionController) {
        self.sessionController = sessionController
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "备份状态"
        navigationItem.largeTitleDisplayMode = .never
        buildUI()

        observerID = sessionController.addObserver { [weak self] snapshot in
            self?.render(snapshot: snapshot)
        }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if (isBeingDismissed || isMovingFromParent), let observerID {
            sessionController.removeObserver(observerID)
            self.observerID = nil
        }
    }

    private func buildUI() {
        stateLabel.font = .systemFont(ofSize: 16, weight: .semibold)
        stateLabel.textColor = .label
        stateLabel.numberOfLines = 0

        progressLabel.font = .systemFont(ofSize: 14, weight: .medium)
        progressLabel.textColor = .secondaryLabel

        logTextView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        logTextView.isEditable = false
        logTextView.backgroundColor = .secondarySystemBackground
        logTextView.layer.cornerRadius = 10
        logTextView.textContainerInset = UIEdgeInsets(top: 8, left: 8, bottom: 8, right: 8)

        startButton.configuration = .filled()
        startButton.configuration?.title = "开始/继续"
        startButton.addTarget(self, action: #selector(startTapped), for: .touchUpInside)

        pauseButton.configuration = .tinted()
        pauseButton.configuration?.title = "暂停"
        pauseButton.addTarget(self, action: #selector(pauseTapped), for: .touchUpInside)

        stopButton.configuration = .tinted()
        stopButton.configuration?.title = "中止"
        stopButton.configuration?.baseForegroundColor = .systemRed
        stopButton.addTarget(self, action: #selector(stopTapped), for: .touchUpInside)

        let buttonRow = UIStackView(arrangedSubviews: [startButton, pauseButton, stopButton])
        buttonRow.axis = .horizontal
        buttonRow.spacing = 8
        buttonRow.distribution = .fillEqually

        let stack = UIStackView(arrangedSubviews: [stateLabel, progressLabel, buttonRow, logTextView])
        stack.axis = .vertical
        stack.spacing = 12

        view.addSubview(stack)
        stack.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(12)
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(view.safeAreaLayoutGuide).inset(12)
        }
    }

    private func render(snapshot: BackupSessionController.Snapshot) {
        stateLabel.text = "状态：\(snapshot.statusText)"
        if snapshot.total > 0 {
            progressLabel.text = "进度：\(snapshot.completed)/\(snapshot.total)"
        } else {
            progressLabel.text = "进度：--"
        }

        if snapshot.logs.count < renderedLogCount {
            logTextView.text = snapshot.logs.joined(separator: "\n")
            renderedLogCount = snapshot.logs.count
        } else if snapshot.logs.count > renderedLogCount {
            let newLines = snapshot.logs[renderedLogCount...].joined(separator: "\n")
            if logTextView.text.isEmpty {
                logTextView.text = newLines
            } else {
                logTextView.text.append("\n" + newLines)
            }
            renderedLogCount = snapshot.logs.count
        }

        if renderedLogCount > 0 {
            let range = NSRange(location: max(logTextView.text.count - 1, 0), length: 1)
            logTextView.scrollRangeToVisible(range)
        }

        switch snapshot.state {
        case .running:
            startButton.isEnabled = false
            pauseButton.isEnabled = true
            stopButton.isEnabled = true
        case .idle, .paused, .stopped, .failed, .completed:
            startButton.isEnabled = true
            pauseButton.isEnabled = false
            stopButton.isEnabled = snapshot.state == .paused
        }
    }

    @objc
    private func startTapped() {
        sessionController.startBackup()
    }

    @objc
    private func pauseTapped() {
        sessionController.pauseBackup()
    }

    @objc
    private func stopTapped() {
        sessionController.stopBackup()
    }
}
