import Foundation
import XCTest
@testable import WatermelonMac

final class LeftoverManifestMaintenanceTests: XCTestCase {
    func testEnumerationAcceptsOnlyCanonicalManifestFiles()
        async throws
    {
        let root = try makeTemporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let basePath = "/Archive"
        let monthsDirectory = root
            .appendingPathComponent("Archive")
            .appendingPathComponent(".watermelon")
            .appendingPathComponent("months")
        try FileManager.default.createDirectory(
            at: monthsDirectory,
            withIntermediateDirectories: true
        )
        try Data().write(
            to: monthsDirectory.appendingPathComponent(
                "2026-07.sqlite"
            )
        )
        try Data().write(
            to: monthsDirectory.appendingPathComponent(
                "2025-12.sqlite"
            )
        )
        try Data().write(
            to: monthsDirectory.appendingPathComponent(
                "2026-07.sqlite.token.tmp"
            )
        )
        try Data().write(
            to: monthsDirectory.appendingPathComponent(
                "2026-13.sqlite"
            )
        )
        try FileManager.default.createDirectory(
            at: monthsDirectory.appendingPathComponent(
                "2024-01.sqlite"
            ),
            withIntermediateDirectories: false
        )
        let client = try LocalVolumeClient(
            connectedRootURL: root
        )

        let months = try await LeftoverManifestMaintenance
            .enumerateMonths(
                client: client,
                basePath: basePath,
                monthsListing: nil
            )

        XCTAssertEqual(
            months,
            [
                LibraryMonthKey(year: 2025, month: 12),
                LibraryMonthKey(year: 2026, month: 7),
            ]
        )
    }

    func testMissingMonthsDirectoryIsAnEmptyEnumeration()
        async throws
    {
        let root = try makeTemporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let client = try LocalVolumeClient(
            connectedRootURL: root
        )

        let months = try await LeftoverManifestMaintenance
            .enumerateMonths(
                client: client,
                basePath: "/Archive",
                monthsListing: nil
            )

        XCTAssertTrue(months.isEmpty)
    }

    func testEnumerationFailsClosedWhenStorageDisappears()
        async throws
    {
        let root = try makeTemporaryRoot()
        let client = try LocalVolumeClient(
            connectedRootURL: root
        )
        try FileManager.default.removeItem(at: root)

        do {
            _ = try await LeftoverManifestMaintenance
                .enumerateMonths(
                    client: client,
                    basePath: "/Archive",
                    monthsListing: nil
                )
            XCTFail("Expected unavailable storage to throw")
        } catch {
            XCTAssertNotEqual(
                RemoteFaultLite.classify(error),
                .notFound
            )
        }
    }

    func testMissingManifestReturnsNoSnapshot() async throws {
        let root = try makeTemporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let client = try LocalVolumeClient(
            connectedRootURL: root
        )
        let provider = LeftoverManifestMaintenance
            .makeSnapshotProvider(
                client: client,
                basePath: "/Archive"
            )

        let snapshot = try await provider(
            LibraryMonthKey(year: 2026, month: 7)
        )

        XCTAssertNil(snapshot)
    }

    func testManifestReadFailsClosedWhenStorageDisappears()
        async throws
    {
        let root = try makeTemporaryRoot()
        let client = try LocalVolumeClient(
            connectedRootURL: root
        )
        let provider = LeftoverManifestMaintenance
            .makeSnapshotProvider(
                client: client,
                basePath: "/Archive"
            )
        try FileManager.default.removeItem(at: root)

        do {
            _ = try await provider(
                LibraryMonthKey(year: 2026, month: 7)
            )
            XCTFail("Expected unavailable storage to throw")
        } catch {
            XCTAssertEqual(
                RemoteFaultLite.classify(error),
                .retryable
            )
        }
    }

    private func makeTemporaryRoot() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "watermelon-leftover-\(UUID().uuidString)"
            )
        try FileManager.default.createDirectory(
            at: url,
            withIntermediateDirectories: false
        )
        return url
    }
}
