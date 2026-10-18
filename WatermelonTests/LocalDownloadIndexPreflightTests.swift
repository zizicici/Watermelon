import XCTest
@testable import Watermelon

@MainActor
final class LocalDownloadIndexPreflightTests: XCTestCase {
    private final class Builder: LocalHashIndexBuilding {
        var outcomes: [Result<LocalHashIndexBuildResult, Error>]
        var calls: [(ids: Set<String>, workers: Int, network: Bool)] = []

        init(_ outcomes: Result<LocalHashIndexBuildResult, Error>...) {
            self.outcomes = outcomes
        }

        func buildIndex(for assetIDs: Set<String>, workerCount: Int, allowNetworkAccess: Bool,
                        progressHandler: LocalHashIndexProgressHandler?, tickHandler: LocalHashIndexProgressTickHandler?) async throws -> LocalHashIndexBuildResult {
            calls.append((assetIDs, workerCount, allowNetworkAccess))
            return try outcomes.removeFirst().get()
        }
    }

    private func result(ready: Set<String> = [], unavailable: Set<String> = [], failed: Set<String> = [],
                        missing: Set<String> = [], networkPending: Set<String> = []) -> LocalHashIndexBuildResult {
        .init(requestedAssetIDs: ready.union(unavailable).union(failed).union(missing),
              readyAssetIDs: ready, unavailableAssetIDs: unavailable, failedAssetIDs: failed,
              missingAssetIDs: missing, networkPendingAssetIDs: networkPending)
    }

    func testSuccessfulOfflinePassBuildsAllRequestedIDs() async throws {
        let builder = Builder(.success(result(ready: ["invalidated", "new-identifier"])))
        try await LocalDownloadIndexPreflight.run(assetIDs: ["invalidated", "new-identifier"],
            buildService: builder, iCloudPhotoBackupMode: .enable)
        XCTAssertEqual(builder.calls.count, 1)
        XCTAssertEqual(builder.calls[0].ids, ["invalidated", "new-identifier"])
        XCTAssertEqual(builder.calls[0].workers, 2)
        XCTAssertFalse(builder.calls[0].network)
    }

    func testUnavailableOriginalsBlockDownloadWhenICloudIsDisabled() async throws {
        let builder = Builder(.success(result(ready: ["rebuilt"], unavailable: ["cloud"])))
        do {
            try await LocalDownloadIndexPreflight.run(assetIDs: ["rebuilt", "cloud"],
                buildService: builder, iCloudPhotoBackupMode: .disable)
            XCTFail("An incomplete index cannot prove that the remote asset is absent locally")
        } catch let error as LocalIndexIncompleteError {
            XCTAssertEqual(error.result.incompleteAssetIDs, ["cloud"])
            XCTAssertEqual(error.result.readyAssetIDs, ["rebuilt"])
        }
        XCTAssertEqual(builder.calls.count, 1)
        XCTAssertFalse(builder.calls[0].network)
    }

    func testEnabledICloudFetchesOnlyUnavailableIDsAfterOfflinePass() async throws {
        let builder = Builder(.success(result(ready: ["local"], unavailable: ["cloud"])),
                              .success(result(ready: ["cloud"])))
        try await LocalDownloadIndexPreflight.run(assetIDs: ["local", "cloud"],
            buildService: builder, iCloudPhotoBackupMode: .enable)
        XCTAssertEqual(builder.calls.count, 2)
        XCTAssertEqual(builder.calls[0].ids, ["local", "cloud"])
        XCTAssertFalse(builder.calls[0].network)
        XCTAssertEqual(builder.calls[1].ids, ["cloud"])
        XCTAssertEqual(builder.calls[1].workers, 1)
        XCTAssertTrue(builder.calls[1].network)
    }

    func testCloudRecoveryDoesNotHideFailedLocalAssets() async throws {
        let builder = Builder(.success(result(unavailable: ["cloud"], failed: ["broken"])),
                              .success(result(ready: ["cloud"])))
        do {
            try await LocalDownloadIndexPreflight.run(assetIDs: ["broken", "cloud"],
                buildService: builder, iCloudPhotoBackupMode: .enable)
            XCTFail("A failed local fingerprint must still block import")
        } catch let error as LocalIndexIncompleteError {
            XCTAssertEqual(error.result.incompleteAssetIDs, ["broken"])
        }
    }

    func testStillUnavailableCloudAssetBlocksDownload() async throws {
        let builder = Builder(.success(result(unavailable: ["cloud"])),
                              .success(result(unavailable: ["cloud"])))
        do {
            try await LocalDownloadIndexPreflight.run(assetIDs: ["cloud"],
                buildService: builder, iCloudPhotoBackupMode: .enable)
            XCTFail("Network access does not guarantee a complete index")
        } catch let error as LocalIndexIncompleteError {
            XCTAssertEqual(error.result.incompleteAssetIDs, ["cloud"])
        }
    }

    func testValidOffloadedFingerprintAndDeletedAssetDoNotBlockDownload() async throws {
        let builder = Builder(.success(result(ready: ["cached"], missing: ["deleted"], networkPending: ["cached"])))
        try await LocalDownloadIndexPreflight.run(assetIDs: ["cached", "deleted"],
            buildService: builder, iCloudPhotoBackupMode: .disable)
        XCTAssertEqual(builder.calls.count, 1)
    }

    func testCancellationStopsBeforeCloudPass() async throws {
        let builder = Builder(.failure(CancellationError()))
        do {
            try await LocalDownloadIndexPreflight.run(assetIDs: ["cloud"],
                buildService: builder, iCloudPhotoBackupMode: .enable)
            XCTFail("Cancellation must abort the download gate")
        } catch is CancellationError { }
        XCTAssertEqual(builder.calls.count, 1)
    }
}
