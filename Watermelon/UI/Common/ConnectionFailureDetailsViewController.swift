import UIKit

struct ConnectionFailurePresentation {
    let summary: String
    let details: String

    init(profile: ServerProfileRecord? = nil, storageType: StorageType? = nil, error: Error,
         context: String? = nil, now: Date = Date()) {
        let failure = error as? StorageConnectionFailure
        let connectionError = failure?.underlying ?? error
        let recovery = connectionError as? BackupNetworkRecoveryExhausted
        let underlying = recovery?.underlying ?? connectionError
        let entries = failure?.diagnostics ?? []
        let fullMessage = UserFacingErrorLocalizer.message(for: underlying, profile: profile, storageType: storageType)
        let reason = fullMessage.components(separatedBy: .newlines)
            .first { !$0.trimmingCharacters(in: .whitespaces).isEmpty } ?? fullMessage
        summary = Self.specificSummary(error: underlying, entries: entries, reason: reason) ?? reason

        let version = Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String ?? "?"
        let build = Bundle.main.object(forInfoDictionaryKey: "CFBundleVersion") as? String ?? "?"
        var lines = [String(localized: "connection.failure.details.privacyNotice"), "",
                     "Watermelon connection diagnostics", "app: \(version) (\(build))",
                     "os: \(UIDevice.current.systemVersion)", "backend: \((profile?.resolvedStorageType ?? storageType)?.rawValue ?? "unknown")",
                     "time: \(ISO8601DateFormatter().string(from: now))",
                     "error: \(RepoFailureDiagnostics.errorCodes(underlying).joined(separator: " -> "))"]
        if recovery != nil { lines.append("recovery: exhausted") }
        if let context { lines.append("context: \(context)") }
        if summary != reason {
            lines.append("summary: \(summary)")
        }
        lines.append("message: \(fullMessage)")
        lines += RepoFailureDiagnostics.errorDetails(underlying)
        for (index, entry) in entries.enumerated() {
            lines += ["", "\(index + 1). \(entry.stage)", "reason: \(entry.issue.rawValue)"]
            if entry.recovered { lines.append("recovered: true") }
            if entry.handled { lines.append("handled: true") }
            if let path = entry.path { lines.append("file: \(path)") }
            if !entry.causes.isEmpty { lines.append("cause: \(entry.causes.joined(separator: " -> "))") }
            if let message = entry.message { lines.append("message: \(message)") }
            lines += entry.underlyingDetails
            if let elapsed = entry.elapsedSeconds {
                lines.append(String(format: "elapsed_seconds: %.3f", locale: Locale(identifier: "en_US_POSIX"), elapsed))
            }
            if let detail = entry.detail { lines.append(detail) }
        }
        details = lines.joined(separator: "\n")
    }

    private static func specificSummary(error: Error, entries: [RepoFailureDiagnostics.Entry], reason: String) -> String? {
        let entries = entries.filter { !$0.recovered && !$0.handled }
        let detail: String?
        if let lite = error as? LiteRepoError {
            detail = liteDetail(lite, entries: entries)
        } else {
            // Keep the first unrecovered failure when parallel steps fail with the same error.
            let codes = RepoFailureDiagnostics.errorCodes(error)
            detail = entries.lazy
                .filter { $0.issue == .operationFailed && $0.causes == codes }
                .compactMap { message(for: $0) }
                .first
        }
        return detail.map { "\($0)\n\(reason)" }
    }

    private static func liteDetail(_ lite: LiteRepoError, entries: [RepoFailureDiagnostics.Entry]) -> String? {
        switch lite {
        case .repoDamaged:
            for issue in [RepoFailureDiagnostics.Issue.versionUnreadable, .versionInvalid, .monthPathConflict, .unexpectedEntry, .versionMissing] {
                if let entry = entries.last(where: { $0.issue == issue }), let message = message(for: entry) { return message }
            }
        case .upgradeMonthUnavailable(let month):
            let matching = entries.filter { $0.path?.contains("/\(month).sqlite") == true }
            for issue in [RepoFailureDiagnostics.Issue.recoveryReadFailed, .recoveryWriteFailed,
                          .recoveryVerificationFailed, .recoveryAmbiguous, .recoveryInvalid] {
                if let entry = matching.last(where: { $0.issue == issue }), let message = message(for: entry) {
                    return message
                }
            }
        default:
            break
        }
        return nil
    }

    private static func message(for entry: RepoFailureDiagnostics.Entry) -> String? {
        let filename = entry.path.map { ($0 as NSString).lastPathComponent } ?? ""
        let month = RepoLayoutLite.month(fromFilename: filename)
            ?? RepoLayoutLite.month(fromScratchFilename: filename)
        func monthMessage(_ key: String.LocalizationValue) -> String? {
            guard let month else { return nil }
            return String.localizedStringWithFormat(String(localized: key), month.text)
        }
        switch entry.issue {
        case .versionInvalid: return String(localized: "connection.failure.versionInvalid")
        case .versionUnreadable: return String(localized: "connection.failure.versionUnreadable")
        case .versionMissing: return String(localized: "connection.failure.versionMissing")
        case .unexpectedEntry: return String(localized: "connection.failure.unexpectedEntry")
        case .monthPathConflict: return monthMessage("connection.failure.monthPathConflict")
        case .recoveryReadFailed:
            let isCanonical = RepoLayoutLite.month(fromFilename: filename) != nil
            return monthMessage(isCanonical ? "connection.failure.loadMonth" : "connection.failure.recoveryReadFailed")
        case .recoveryWriteFailed, .recoveryVerificationFailed: return monthMessage("connection.failure.recoveryWriteFailed")
        case .recoveryAmbiguous: return monthMessage("connection.failure.recoveryAmbiguous")
        case .recoveryInvalid: return monthMessage("connection.failure.recoveryInvalid")
        case .operationFailed:
            switch entry.stage {
            case "connection.prepare": return String(localized: "connection.failure.prepare")
            case "connection.sftp_host_key": return String(localized: "connection.failure.sftpHostKey")
            case "connection.transport", "connection.sftp_tcp", "connection.sftp_handshake",
                 "connection.sftp_subsystem", "connection.sftp_authentication_setup":
                return String(localized: "connection.failure.transport")
            case "connection.reload_index": return String(localized: "connection.failure.reloadIndex")
            case "upgrade.load_month": return monthMessage("connection.failure.loadMonth")
            case "upgrade.publish_month": return monthMessage("connection.failure.writeMonth")
            case "upgrade.thumbnails": return monthMessage("connection.failure.thumbnails")
            case "upgrade.write_pending", "upgrade.write_recovery_marker", "upgrade.commit_version":
                return String(localized: "connection.failure.versionWrite")
            case "upgrade.verify_recovery_marker": return String(localized: "connection.failure.versionVerify")
            default: return nil
            }
        default: return nil
        }
    }
}

extension UIViewController {
    func presentStorageError(title: String, error: Error, storageType: StorageType) {
        let underlying = (error as? StorageConnectionFailure)?.underlying ?? error
        guard RemoteFaultLite.classify(StorageErrorMessages.unwrapped(underlying)) != .cancelled else { return }
        let report = ConnectionFailurePresentation(storageType: storageType, error: error, context: title)
        let alert = ConnectionFailureAlertFactory.make(report: report, title: title, onDetails: { [weak self] report in
            self?.present(AppNavigationController(rootViewController: ConnectionFailureDetailsViewController(report: report)), animated: true)
        })
        present(alert, animated: true)
    }
}

final class ConnectionFailureDetailsViewController: UIViewController {
    private let report: ConnectionFailurePresentation

    init(report: ConnectionFailurePresentation) {
        self.report = report
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "connection.failure.details")
        view.backgroundColor = .systemBackground
        navigationItem.leftBarButtonItem = UIBarButtonItem(systemItem: .close,
            primaryAction: UIAction { [weak self] _ in self?.dismiss(animated: ConsideringUser.animated) })
        navigationItem.rightBarButtonItem = UIBarButtonItem(title: String(localized: "common.copy"),
            style: .plain, target: self, action: #selector(copyDetails))
        let textView = UITextView()
        textView.isEditable = false
        textView.isSelectable = true
        textView.font = UIFont.preferredFont(forTextStyle: .body)
        textView.adjustsFontForContentSizeCategory = true
        textView.textColor = .label
        textView.backgroundColor = .systemBackground
        textView.textContainerInset = UIEdgeInsets(top: 16, left: 16, bottom: 24, right: 16)
        textView.text = report.details
        textView.accessibilityIdentifier = "connection.failure.detailsText"
        textView.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(textView)
        NSLayoutConstraint.activate([
            textView.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor),
            textView.leadingAnchor.constraint(equalTo: view.safeAreaLayoutGuide.leadingAnchor),
            textView.trailingAnchor.constraint(equalTo: view.safeAreaLayoutGuide.trailingAnchor),
            textView.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor)
        ])
    }

    @objc private func copyDetails() {
        UIPasteboard.general.string = report.details
        navigationItem.rightBarButtonItem?.title = String(localized: "connection.failure.copied")
        UIAccessibility.post(notification: .announcement, argument: String(localized: "connection.failure.copied"))
    }
}
