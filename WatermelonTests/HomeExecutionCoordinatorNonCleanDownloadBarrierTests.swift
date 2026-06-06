import XCTest
@testable import Watermelon

/// R06 CodexReviewerB: a non-clean V2 materialization (ambiguous/corrupt) exposes only best-effort
/// rows through the committed view. `HomeExecutionCoordinator.downloadRemoteMonth` must fail closed on
/// such a month *before* it builds restore candidates, so a download/sync can never be reported complete
/// on a non-authoritative V2 view. The barrier can't be exercised at runtime (the method opens a live
/// remote through `verifyMonth`), so it is pinned at the source level — mirroring the coordinator
/// invariant tests in `AppRuntimeFlagsGateTests`.
final class HomeExecutionCoordinatorNonCleanDownloadBarrierTests: XCTestCase {

    private func repoRoot() -> URL {
        URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
    }

    func testDownloadRemoteMonthFailsClosedBeforeBuildingRemoteItems() throws {
        let url = repoRoot().appendingPathComponent("Watermelon/Home/HomeExecutionCoordinator.swift")
        let source = try String(contentsOf: url, encoding: .utf8)

        // Scope the assertions to downloadRemoteMonth so an occurrence elsewhere can't pass them.
        guard let methodStart = source.range(of: "private func downloadRemoteMonth(") else {
            XCTFail("downloadRemoteMonth(...) declaration not found")
            return
        }
        let body = String(source[methodStart.lowerBound...])

        guard let barrierIdx = body.range(of: "nonCleanOutcomeMonths().contains(month)") else {
            XCTFail("downloadRemoteMonth must fail closed when the month's V2 materialization is non-clean")
            return
        }
        guard let remoteItemsIdx = body.range(of: "dataAccess.remoteOnlyItems(month)") else {
            XCTFail("downloadRemoteMonth must still build remote items on the clean path")
            return
        }
        XCTAssertTrue(
            barrierIdx.lowerBound < remoteItemsIdx.lowerBound,
            "the non-clean barrier must precede remoteOnlyItems so best-effort rows are never restored as a completed month"
        )

        let barrierToItems = String(body[barrierIdx.lowerBound..<remoteItemsIdx.lowerBound])
        XCTAssertTrue(
            barrierToItems.contains("return .failed("),
            "a non-clean month must return a failed download result, never fall through to a completed restore"
        )
    }

    /// The fail-closed message is user-facing; per the catalog's all-locales rule a missing translation
    /// is a shipped English-fallback bug, so pin parity against a representative existing key.
    func testUnverifiedRemoteStateStringIsLocalizedForEveryCatalogLocale() throws {
        let url = repoRoot().appendingPathComponent("Watermelon/Resource/Localizable.xcstrings")
        let json = try JSONSerialization.jsonObject(with: try Data(contentsOf: url)) as? [String: Any]
        let strings = json?["strings"] as? [String: Any]

        func locales(_ key: String) -> Set<String> {
            let entry = strings?[key] as? [String: Any]
            let localizations = entry?["localizations"] as? [String: Any]
            return Set((localizations ?? [:]).keys)
        }

        let provided = locales("home.execution.download.unverifiedRemoteState")
        XCTAssertFalse(provided.isEmpty, "the non-clean download failure message must exist in the catalog")
        XCTAssertEqual(
            provided,
            locales("home.execution.notConnected"),
            "every shipped locale needs a translation for the non-clean download failure message"
        )
    }
}
