import UIKit
import XCTest
@testable import Watermelon

final class ConnectionFailureDetailsTests: XCTestCase {
    private let path = "/private-account/photos/.watermelon/months/2026-01.sqlite.first.bak"

    private func profile() -> ServerProfileRecord {
        ServerProfileRecord(id: nil, name: "private-name", storageType: "webdav", connectionParams: nil,
            sortOrder: 0, host: "private-host", port: 443, shareName: "", basePath: "/private-account/photos",
            username: "private-user", domain: nil, credentialRef: "private-credential", backgroundBackupEnabled: false,
            backgroundBackupMinIntervalMinutes: 720, backgroundBackupRequiresWiFi: false,
            generateRemoteThumbnails: false, createdAt: Date(), updatedAt: Date())
    }

    private func liteSummary(_ detail: String, _ error: LiteRepoError) -> String {
        "\(detail)\n\(error.localizedDescription)"
    }

    private func failure(_ issue: RepoFailureDiagnostics.Issue = .recoveryReadFailed) -> StorageConnectionFailure {
        let recorder = RepoFailureDiagnostics()
        RepoFailureDiagnostics.$current.withValue(recorder) {
            RepoFailureDiagnostics.record(issue, stage: "recovery.download", path: path,
                error: RemoteStorageClientError.underlying(NSError(domain: "WebDAVClient", code: 403,
                    userInfo: [NSLocalizedDescriptionKey: "secret-token", NSURLErrorFailingURLStringErrorKey: "https://user:password@host/?token=secret"])))
        }
        return StorageConnectionFailure(underlying: LiteRepoError.upgradeMonthUnavailable(month: "2026-01"),
                                        diagnostics: recorder.snapshot())
    }

    @MainActor
    func testSummaryIdentifiesUnreadableRecoveryAndReportKeepsFullDiagnostics() {
        let report = ConnectionFailurePresentation(profile: profile(), error: failure())
        XCTAssertEqual(report.summary, liteSummary(String.localizedStringWithFormat(String(localized: "connection.failure.recoveryReadFailed"), "2026-01"), .upgradeMonthUnavailable(month: "2026-01")))
        XCTAssertFalse(report.summary.contains("403"))
        XCTAssertTrue(report.details.contains("recovery.download"))
        XCTAssertTrue(report.details.contains("cause: WebDAVClient/403"))
        XCTAssertTrue(report.details.contains("file: \(path)"))
        XCTAssertTrue(report.details.contains("message: secret-token"))
        XCTAssertFalse(report.details.contains("private-credential"))
        XCTAssertFalse(report.details.contains("password"))
    }

    @MainActor
    func testLongSummaryIsShownInFull() {
        let long = String(repeating: "很长的错误说明", count: 30)
        let report = ConnectionFailurePresentation(profile: profile(),
            error: NSError(domain: "Test", code: 7, userInfo: [NSLocalizedDescriptionKey: long]))
        XCTAssertEqual(report.summary, long)
    }

    @MainActor
    func testDetailsStartWithPrivacyNoticeAndKeepErrorTextVerbatim() {
        let message = "COPY 请求失败（405）：https://private-host/private-account/photos/x.sqlite\n第二行：private-host:443 private-user"
        let report = ConnectionFailurePresentation(profile: profile(),
            error: RemoteStorageClientError.underlying(NSError(domain: WebDAVClient.errorDomain, code: 405,
                userInfo: [NSLocalizedDescriptionKey: message])))
        XCTAssertEqual(report.summary, "COPY 请求失败（405）：https://private-host/private-account/photos/x.sqlite")
        XCTAssertEqual(report.details.components(separatedBy: "\n").first,
                       String(localized: "connection.failure.details.privacyNotice"))
        XCTAssertTrue(report.details.contains("error: WebDAVClient/405"))
        XCTAssertTrue(report.details.contains("message: \(message)"))
    }

    @MainActor
    func testStepContextKeepsTheThrownErrorsReason() {
        let timeout = NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut)
        let other = NSError(domain: NSURLErrorDomain, code: NSURLErrorNetworkConnectionLost)
        let recorder = RepoFailureDiagnostics()
        RepoFailureDiagnostics.$current.withValue(recorder) {
            RepoFailureDiagnostics.record(.operationFailed, stage: "upgrade.publish_month",
                path: "/photos/.watermelon/months/2026-01.sqlite", error: timeout)
            RepoFailureDiagnostics.record(.operationFailed, stage: "upgrade.load_month",
                path: "/photos/.watermelon/months/2026-02.sqlite", error: other)
        }
        let report = ConnectionFailurePresentation(profile: profile(),
            error: StorageConnectionFailure(underlying: timeout, diagnostics: recorder.snapshot()))
        let reason = profile().userFacingStorageErrorMessage(timeout).components(separatedBy: .newlines).first ?? ""
        let context = String.localizedStringWithFormat(String(localized: "connection.failure.writeMonth"), "2026-01")
        XCTAssertEqual(report.summary, "\(context)\n\(reason)")
    }

    @MainActor
    func testStepContextIsSkippedWhenNoStepThrewThatError() {
        let timeout = NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut)
        let recorder = RepoFailureDiagnostics()
        RepoFailureDiagnostics.$current.withValue(recorder) {
            RepoFailureDiagnostics.record(.operationFailed, stage: "upgrade.publish_month",
                path: "/photos/.watermelon/months/2026-01.sqlite",
                error: NSError(domain: "MonthManifestStore", code: -34))
        }
        let report = ConnectionFailurePresentation(profile: profile(),
            error: StorageConnectionFailure(underlying: timeout, diagnostics: recorder.snapshot()))
        let reason = profile().userFacingStorageErrorMessage(timeout).components(separatedBy: .newlines).first ?? ""
        XCTAssertEqual(report.summary, reason)
    }

    @MainActor
    func testSameErrorInSeveralMonthsNamesTheFirstFailure() {
        let timeout = NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut)
        let recorder = RepoFailureDiagnostics()
        RepoFailureDiagnostics.$current.withValue(recorder) {
            for month in ["2026-01", "2026-02"] {
                RepoFailureDiagnostics.record(.operationFailed, stage: "upgrade.publish_month",
                    path: "/photos/.watermelon/months/\(month).sqlite", error: timeout)
            }
        }
        let report = ConnectionFailurePresentation(profile: profile(),
            error: StorageConnectionFailure(underlying: timeout, diagnostics: recorder.snapshot()))
        XCTAssertTrue(report.summary.hasPrefix(
            String.localizedStringWithFormat(String(localized: "connection.failure.writeMonth"), "2026-01")))
    }

    @MainActor
    func testCanonicalReadFailureIsNotCalledARecoveryFile() {
        let recorder = RepoFailureDiagnostics()
        RepoFailureDiagnostics.$current.withValue(recorder) {
            RepoFailureDiagnostics.record(.recoveryReadFailed, stage: "recovery.metadata",
                path: "/photos/.watermelon/months/2026-01.sqlite")
        }
        let report = ConnectionFailurePresentation(profile: profile(), error: StorageConnectionFailure(
            underlying: LiteRepoError.upgradeMonthUnavailable(month: "2026-01"), diagnostics: recorder.snapshot()))
        XCTAssertEqual(report.summary, liteSummary(String.localizedStringWithFormat(String(localized: "connection.failure.loadMonth"), "2026-01"), .upgradeMonthUnavailable(month: "2026-01")))
    }

    @MainActor
    func testMissingVersionIsOnlyBlamedWhenItCausesTheDamage() async throws {
        let occupied = InMemoryRemoteStorageClient()
        await occupied.seedFile(path: "/photos/.watermelon", data: Data("not a folder".utf8))
        let occupiedRecorder = RepoFailureDiagnostics()
        let occupiedDecision = try await RepoFailureDiagnostics.$current.withValue(occupiedRecorder) {
            try await RepoFormatRouter(client: occupied, basePath: "/photos").classifyForRead()
        }
        XCTAssertEqual(occupiedDecision, .damaged)
        let occupiedReport = ConnectionFailurePresentation(profile: profile(), error: StorageConnectionFailure(
            underlying: LiteRepoError.repoDamaged, diagnostics: occupiedRecorder.snapshot()))
        XCTAssertEqual(occupiedReport.summary, LiteRepoError.repoDamaged.localizedDescription)

        let orphaned = InMemoryRemoteStorageClient()
        await orphaned.seedFile(path: "/photos/.watermelon/months/2026-01.sqlite", data: Data("month".utf8))
        let orphanedRecorder = RepoFailureDiagnostics()
        let orphanedDecision = try await RepoFailureDiagnostics.$current.withValue(orphanedRecorder) {
            try await RepoFormatRouter(client: orphaned, basePath: "/photos").classifyForRead()
        }
        XCTAssertEqual(orphanedDecision, .damaged)
        let orphanedReport = ConnectionFailurePresentation(profile: profile(), error: StorageConnectionFailure(
            underlying: LiteRepoError.repoDamaged, diagnostics: orphanedRecorder.snapshot()))
        XCTAssertEqual(orphanedReport.summary, liteSummary(String(localized: "connection.failure.versionMissing"), .repoDamaged))
    }

    func testVersionReadFaultIsRecordedOnceWithItsPath() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "/photos/.watermelon/version.json", data: Data("{}".utf8))
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)
        let recorder = RepoFailureDiagnostics()
        _ = try? await RepoFailureDiagnostics.$current.withValue(recorder) {
            try await RepoFormatRouter(client: client, basePath: "/photos").classifyForRead()
        }
        let entries = recorder.snapshot()
        XCTAssertEqual(entries.map(\.stage), ["format.probe"])
        XCTAssertEqual(entries.first?.path, "/photos/.watermelon/version.json")
    }

    @MainActor
    func testSummaryUsesFailureForTheAffectedMonth() {
        let recorder = RepoFailureDiagnostics()
        RepoFailureDiagnostics.$current.withValue(recorder) {
            RepoFailureDiagnostics.record(.recoveryAmbiguous, stage: "recovery.select", path: path)
            RepoFailureDiagnostics.record(.recoveryReadFailed, stage: "recovery.download",
                path: "/photos/.watermelon/months/2026-02.sqlite.other.tmp")
        }
        let error = StorageConnectionFailure(underlying: LiteRepoError.upgradeMonthUnavailable(month: "2026-01"), diagnostics: recorder.snapshot())
        let report = ConnectionFailurePresentation(profile: profile(), error: error)
        XCTAssertEqual(report.summary, liteSummary(String.localizedStringWithFormat(String(localized: "connection.failure.recoveryAmbiguous"), "2026-01"), .upgradeMonthUnavailable(month: "2026-01")))
    }

    func testReadBackRetryExhaustionKeepsTheUnderlyingStatus() {
        let exhausted = RemoteReadBackRetryExhaustedError(
            underlying: RemoteStorageClientError.underlying(NSError(domain: WebDAVClient.errorDomain, code: 404)))
        XCTAssertEqual(RepoFailureDiagnostics.errorCodes(exhausted), ["RemoteReadBackRetryExhausted", "WebDAVClient/404"])
    }

    func testCapturePreservesOriginalErrorAndNestedCodes() async {
        let recorder = RepoFailureDiagnostics()
        do {
            try await RepoFailureDiagnostics.$current.withValue(recorder) {
                try await RepoFailureDiagnostics.capture(stage: "upgrade.publish_month", path: path) {
                    throw LiteRepoError.ownershipLost
                }
            }
            XCTFail("The original failure must propagate")
        } catch {
            XCTAssertEqual(error as? LiteRepoError, .ownershipLost)
        }
        XCTAssertEqual(recorder.snapshot().first?.causes, ["LiteRepoError/ownershipLost"])
        let nested = NSError(domain: "MonthManifestStore", code: -34,
            userInfo: [NSUnderlyingErrorKey: NSError(domain: "SQLite", code: 26)])
        XCTAssertEqual(RepoFailureDiagnostics.errorCodes(nested), ["MonthManifestStore/-34", "SQLite/26"])
    }

    func testDiagnosticsAreBoundedAndConnectionScopesAreIsolated() async {
        let first = RepoFailureDiagnostics()
        let second = RepoFailureDiagnostics()
        await withTaskGroup(of: Void.self) { group in
            for (recorder, stage) in [(first, "first"), (second, "second")] {
                group.addTask {
                    RepoFailureDiagnostics.$current.withValue(recorder) {
                        for index in 1...40 { RepoFailureDiagnostics.record(.operationFailed, stage: stage, path: "\(index)") }
                    }
                }
            }
        }
        XCTAssertEqual(first.snapshot().map(\.path), (9...40).map { "\($0)" }, "Only the most recent 32 are kept")
        XCTAssertEqual(Set(first.snapshot().map(\.stage)), ["first"])
        XCTAssertEqual(Set(second.snapshot().map(\.stage)), ["second"])
        XCTAssertNil(RepoFailureDiagnostics.current)
    }

    func testIdenticalRecordsAreKeptOnce() {
        let recorder = RepoFailureDiagnostics()
        RepoFailureDiagnostics.$current.withValue(recorder) {
            for _ in 0..<2 { RepoFailureDiagnostics.record(.unexpectedEntry, stage: "format.directory", path: path) }
            RepoFailureDiagnostics.record(.unexpectedEntry, stage: "format.months", path: path)
        }
        XCTAssertEqual(recorder.snapshot().map(\.stage), ["format.directory", "format.months"])
    }

    func testIgnoredMonthsListingFaultIsNotRecorded() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "/photos/.watermelon/version.json",
            data: try VersionManifestLite.encode(VersionManifestLite.makeManifest(createdAt: "date", createdBy: "writer")))
        await client.seedFile(path: "/photos/.watermelon/months/2026-01.sqlite", data: Data("month".utf8))
        let baseEntries = try await client.list(path: "/photos")
        let repoEntries = try await client.list(path: "/photos/.watermelon")
        await client.enqueueListResult(baseEntries)
        await client.enqueueListResult(repoEntries)
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        let recorder = RepoFailureDiagnostics()
        let probe = try await RepoFailureDiagnostics.$current.withValue(recorder) {
            try await RepoFormatRouter(client: client, basePath: "/photos").classifyDetailed()
        }
        XCTAssertEqual(probe.decision, .current)
        XCTAssertEqual(recorder.snapshot().map(\.stage), [])
    }

    @MainActor
    func testBlankFirstLineFallsBackToTheNextLine() {
        let report = ConnectionFailurePresentation(profile: profile(),
            error: RemoteStorageClientError.underlying(NSError(domain: WebDAVClient.errorDomain, code: 7,
                userInfo: [NSLocalizedDescriptionKey: ""])))
        XCTAssertEqual(report.summary, "(\(WebDAVClient.errorDomain) / 7)")
    }

    @MainActor
    func testUnmappableEarlierStepWithTheSameErrorIsSkipped() {
        let timeout = NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut)
        let recorder = RepoFailureDiagnostics()
        RepoFailureDiagnostics.$current.withValue(recorder) {
            RepoFailureDiagnostics.record(.operationFailed, stage: "format.probe",
                path: "/photos/.watermelon/months", error: timeout)
            RepoFailureDiagnostics.record(.operationFailed, stage: "upgrade.load_month",
                path: "/photos/.watermelon/months/2026-01.sqlite", error: timeout)
        }
        let report = ConnectionFailurePresentation(profile: profile(),
            error: StorageConnectionFailure(underlying: timeout, diagnostics: recorder.snapshot()))
        XCTAssertTrue(report.summary.hasPrefix(
            String.localizedStringWithFormat(String(localized: "connection.failure.loadMonth"), "2026-01")))
    }

    @MainActor
    func testUnnamedCauseFallsThroughToTheNextOne() {
        let recorder = RepoFailureDiagnostics()
        RepoFailureDiagnostics.$current.withValue(recorder) {
            RepoFailureDiagnostics.record(.recoveryReadFailed, stage: "recovery.download",
                path: "/photos/.watermelon/months/2026-01.sqlite/nested")
            RepoFailureDiagnostics.record(.recoveryInvalid, stage: "recovery.validate",
                path: "/photos/.watermelon/months/2026-01.sqlite.x.tmp")
        }
        let report = ConnectionFailurePresentation(profile: profile(), error: StorageConnectionFailure(
            underlying: LiteRepoError.upgradeMonthUnavailable(month: "2026-01"), diagnostics: recorder.snapshot()))
        XCTAssertEqual(report.summary.components(separatedBy: "\n").first,
            String.localizedStringWithFormat(String(localized: "connection.failure.recoveryInvalid"), "2026-01"))
    }

    @MainActor
    func testEveryRecordedCauseHasItsOwnSummary() {
        func monthText(_ key: String.LocalizationValue) -> String {
            String.localizedStringWithFormat(String(localized: key), "2026-01")
        }
        let month = "/photos/.watermelon/months/2026-01.sqlite"
        let version = "/photos/.watermelon/version.json"
        let unavailable = LiteRepoError.upgradeMonthUnavailable(month: "2026-01")
        let timeout = NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut)
        let cases: [(RepoFailureDiagnostics.Issue, String, String, Error, String)] = [
            (.recoveryWriteFailed, "recovery.upload", month, unavailable, monthText("connection.failure.recoveryWriteFailed")),
            (.recoveryVerificationFailed, "recovery.verify", month, unavailable, monthText("connection.failure.recoveryWriteFailed")),
            (.recoveryInvalid, "recovery.validate", month + ".x.tmp", unavailable, monthText("connection.failure.recoveryInvalid")),
            (.unexpectedEntry, "format.directory", "/photos/.watermelon/notes.txt", LiteRepoError.repoDamaged,
             String(localized: "connection.failure.unexpectedEntry")),
            (.monthPathConflict, "format.months", month, LiteRepoError.repoDamaged, monthText("connection.failure.monthPathConflict")),
            (.operationFailed, "upgrade.thumbnails", month, timeout, monthText("connection.failure.thumbnails")),
            (.operationFailed, "upgrade.write_pending", version, timeout, String(localized: "connection.failure.versionWrite")),
            (.operationFailed, "upgrade.write_recovery_marker", version, timeout, String(localized: "connection.failure.versionWrite")),
            (.operationFailed, "upgrade.commit_version", version, timeout, String(localized: "connection.failure.versionWrite")),
            (.operationFailed, "upgrade.verify_recovery_marker", version, timeout, String(localized: "connection.failure.versionVerify")),
        ]
        for (issue, stage, path, error, expected) in cases {
            let recorder = RepoFailureDiagnostics()
            RepoFailureDiagnostics.$current.withValue(recorder) {
                RepoFailureDiagnostics.record(issue, stage: stage, path: path, error: issue == .operationFailed ? error : nil)
            }
            let report = ConnectionFailurePresentation(profile: profile(),
                error: StorageConnectionFailure(underlying: error, diagnostics: recorder.snapshot()))
            XCTAssertEqual(report.summary.components(separatedBy: "\n").first, expected, stage)
        }
    }

    @MainActor
    func testInvalidVersionProducesSpecificSummary() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "/photos/.watermelon/version.json", data: Data("not json".utf8))
        let recorder = RepoFailureDiagnostics()
        let decision = try await RepoFailureDiagnostics.$current.withValue(recorder) {
            try await RepoFormatRouter(client: client, basePath: "/photos").classifyForRead()
        }
        XCTAssertEqual(decision, .damaged)
        let error = StorageConnectionFailure(underlying: LiteRepoError.repoDamaged, diagnostics: recorder.snapshot())
        let report = ConnectionFailurePresentation(profile: profile(), error: error)
        XCTAssertEqual(report.summary, liteSummary(String(localized: "connection.failure.versionInvalid"), .repoDamaged))
        XCTAssertTrue(report.details.contains(".watermelon/version.json"))
    }

    @MainActor
    func testUnreadableDownloadedVersionIsNotReportedAsInvalidContent() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "/photos/.watermelon/version.json", data: Data("{}".utf8))
        await client.enqueueDownloadWithoutLocalFile()
        let recorder = RepoFailureDiagnostics()
        let decision = try await RepoFailureDiagnostics.$current.withValue(recorder) {
            try await RepoFormatRouter(client: client, basePath: "/photos").classify()
        }
        XCTAssertEqual(decision, .damaged)
        XCTAssertFalse(recorder.snapshot().contains { $0.issue == .versionInvalid })
        XCTAssertFalse(try XCTUnwrap(recorder.snapshot().first { $0.issue == .versionUnreadable }).causes.isEmpty)
        let error = StorageConnectionFailure(underlying: LiteRepoError.repoDamaged, diagnostics: recorder.snapshot())
        let report = ConnectionFailurePresentation(profile: profile(), error: error)
        XCTAssertEqual(report.summary, liteSummary(String(localized: "connection.failure.versionUnreadable"), .repoDamaged))
    }

    @MainActor
    func testAlertKeepsDiagnosticsBehindDetailsAction() {
        let alert = ConnectionFailureAlertFactory.make(profile: profile(), error: failure(), onDetails: { _ in }, onEdit: {})
        XCTAssertEqual(alert.actions.map(\.title), [String(localized: "common.ok"), String(localized: "connection.failure.details"), String(localized: "common.edit")])
        XCTAssertEqual(alert.message, liteSummary(String.localizedStringWithFormat(String(localized: "connection.failure.recoveryReadFailed"), "2026-01"), .upgradeMonthUnavailable(month: "2026-01")))
        XCTAssertFalse(alert.message?.contains("WebDAVClient") ?? true)
    }

    @MainActor
    func testDetailsLayoutAndAlertPreview() async throws {
        let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
        let previous = scene.windows.first { $0.isKeyWindow }
        let window = UIWindow(windowScene: scene)
        let root = UIViewController()
        root.view.backgroundColor = .systemBackground
        window.rootViewController = root
        window.makeKeyAndVisible()
        defer { window.isHidden = true; previous?.makeKeyAndVisible() }
        let alert = ConnectionFailureAlertFactory.make(profile: profile(), error: failure(), onDetails: { _ in }, onEdit: {})
        root.present(alert, animated: false)
        await Task.yield()
        window.layoutIfNeeded()
        attach(window, name: "connection-error-alert")
        root.dismiss(animated: false)

        let report = ConnectionFailurePresentation(profile: profile(), error: failure())
        let details = ConnectionFailureDetailsViewController(report: report)
        let navigation = UINavigationController(rootViewController: details)
        root.addChild(navigation)
        root.view.addSubview(navigation.view)
        navigation.didMove(toParent: root)
        navigation.view.frame = CGRect(x: 0, y: 0, width: 320, height: 568)
        root.setOverrideTraitCollection(UITraitCollection(preferredContentSizeCategory: .accessibilityExtraExtraExtraLarge), forChild: navigation)
        navigation.view.layoutIfNeeded()
        details.view.layoutIfNeeded()
        let text = try XCTUnwrap(details.view.subviews.compactMap { $0 as? UITextView }.first)
        XCTAssertTrue(text.isSelectable)
        XCTAssertFalse(text.isEditable)
        XCTAssertTrue(text.isScrollEnabled)
        XCTAssertGreaterThan(text.bounds.width, 0)
        XCTAssertLessThanOrEqual(text.frame.maxX, details.view.bounds.width)
        XCTAssertLessThanOrEqual(text.frame.maxY, details.view.bounds.height)
        XCTAssertEqual(text.text, report.details)
        let copy = try XCTUnwrap(details.navigationItem.rightBarButtonItem)
        let copyAction = try XCTUnwrap(copy.action)
        XCTAssertTrue(UIApplication.shared.sendAction(copyAction, to: copy.target, from: copy, for: nil))
        XCTAssertEqual(UIPasteboard.general.string, report.details)
        attach(navigation.view, name: "connection-error-details-small-large-text")
    }

    @MainActor
    private func attach(_ view: UIView, name: String) {
        let image = UIGraphicsImageRenderer(bounds: view.bounds).image { _ in
            view.drawHierarchy(in: view.bounds, afterScreenUpdates: true)
        }
        let attachment = XCTAttachment(image: image)
        attachment.name = name
        attachment.lifetime = .keepAlways
        add(attachment)
    }
}
