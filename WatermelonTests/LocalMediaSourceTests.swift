import Foundation
import XCTest
@testable import Watermelon

final class LocalMediaSourceTests: XCTestCase {
    private actor SizeLoader {
        private(set) var requests: [Set<String>] = []

        func load(_ identifiers: Set<String>) -> [String: Int64] {
            requests.append(identifiers)
            return ["sized": 12_345]
        }
    }

    @MainActor
    func testSizeLoadingIsOptInAndPreservesLocalIdentity() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let dependencies = DependencyContainer(
            databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")),
            reconcileOneDriveAccounts: false
        )
        defer { try? dependencies.databaseManager.dbQueue.close() }
        let seed = HomeBrowserLocalSeed(localIDByFingerprint: [:], assets: [
            .init(localIdentifier: "sized", month: .init(year: 2026, month: 1), kind: .livePhoto,
                  creationDateMs: 1, fingerprint: nil),
            .init(localIdentifier: "unknown", month: .init(year: 2026, month: 1), kind: .video,
                  creationDateMs: 2, fingerprint: nil),
        ], monthGroupingTimeZone: .frozenCurrent())
        let presence = LibraryPresenceIndex(
            hashIndexRepository: dependencies.hashIndexRepository,
            coordinator: dependencies.backupCoordinator,
            profileKey: { nil },
            homeLocalSeedProvider: { seed }
        )
        let loader = SizeLoader()
        let source: MediaBrowserSource = LocalMediaSource(
            photoLibraryService: dependencies.photoLibraryService,
            hashIndexRepository: dependencies.hashIndexRepository,
            presenceIndex: presence,
            fileSizeLoader: { await loader.load($0) }
        )
        guard case .loaded(let months) = await source.load(),
              case .loaded(let sized) = await source.load(includeFileSizes: true) else {
            return XCTFail("Expected seeded local content")
        }
        let requests = await loader.requests
        XCTAssertEqual(requests, [["sized", "unknown"]])
        XCTAssertEqual(months.sections.flatMap(\.items).map(\.id), sized.sections.flatMap(\.items).map(\.id))
        XCTAssertTrue(months.sections.flatMap(\.items).allSatisfy { $0.fileSize == nil })
        let byID = Dictionary(uniqueKeysWithValues: sized.sections.flatMap(\.items).map { ($0.id, $0) })
        XCTAssertEqual(byID[.local("sized")]?.fileSize, 12_345)
        XCTAssertNil(byID[.local("unknown")]?.fileSize)
        XCTAssertEqual(byID[.local("sized")]?.kind, .livePhoto)

        let selected = [
            MediaBrowserItem(kind: .photo, creationDateMs: 0, localIdentifier: "sized",
                fingerprint: nil, isBackedUp: false),
            MediaBrowserItem(kind: .photo, creationDateMs: 0, localIdentifier: "cached",
                fingerprint: nil, isBackedUp: false, fileSize: 999),
        ]
        let selectedSizes = await source.fileSizes(for: selected)
        XCTAssertEqual(selectedSizes, [.local("sized"): 12_345, .local("cached"): 999])
        let selectedRequests = await loader.requests
        XCTAssertEqual(selectedRequests, [["sized", "unknown"], ["sized"]])
    }

    func testHomeSeedProjectionGroupsSortsAndAppliesPresence() throws {
        let january = LibraryMonthKey(year: 2026, month: 1)
        let february = LibraryMonthKey(year: 2026, month: 2)
        let backedUp = Data([0x01])
        let seed = HomeBrowserLocalSeed(
            localIDByFingerprint: [backedUp: "jan-new"],
            assets: [
                HomeBrowserLocalAsset(
                    localIdentifier: "jan-old",
                    month: january,
                    kind: .photo,
                    creationDateMs: 1_000,
                    fingerprint: nil
                ),
                HomeBrowserLocalAsset(
                    localIdentifier: "feb",
                    month: february,
                    kind: .video,
                    creationDateMs: 3_000,
                    fingerprint: Data([0x02])
                ),
                HomeBrowserLocalAsset(
                    localIdentifier: "jan-new",
                    month: january,
                    kind: .livePhoto,
                    creationDateMs: 2_000,
                    fingerprint: backedUp
                ),
            ],
            monthGroupingTimeZone: .frozenCurrent()
        )

        let content = try XCTUnwrap(LocalMediaSource.content(
            from: seed,
            backedUpFingerprints: [backedUp]
        ))
        let sections = content.sections

        XCTAssertEqual(sections.map(\.month), [february, january])
        XCTAssertEqual(sections[1].items.map(\.testLabel), ["jan-new", "jan-old"])
        XCTAssertEqual(sections[1].items[0].presence, .both)
        XCTAssertEqual(sections[1].items[1].presence, .localOnly)
        XCTAssertEqual(sections[0].items[0].presence, .localOnly)
        if case .livePhoto = sections[1].items[0].kind {
        } else {
            XCTFail("Expected live photo kind")
        }
    }

    func testHomeSeedProjectionUsesStableIdentifierTieBreak() throws {
        let january = LibraryMonthKey(year: 2026, month: 1)
        let seed = HomeBrowserLocalSeed(
            localIDByFingerprint: [:],
            assets: [
                HomeBrowserLocalAsset(
                    localIdentifier: "z",
                    month: january,
                    kind: .photo,
                    creationDateMs: 1_000,
                    fingerprint: nil
                ),
                HomeBrowserLocalAsset(
                    localIdentifier: "a",
                    month: january,
                    kind: .photo,
                    creationDateMs: 1_000,
                    fingerprint: nil
                ),
            ],
            monthGroupingTimeZone: .frozenCurrent()
        )

        let content = try XCTUnwrap(LocalMediaSource.content(
            from: seed,
            backedUpFingerprints: []
        ))
        let sections = content.sections

        XCTAssertEqual(sections[0].items.map(\.testLabel), ["a", "z"])
    }

    func testHomeSeedProjectionCanBuildOnlyLocalOnlyItemsForMergedMode() throws {
        let january = LibraryMonthKey(year: 2026, month: 1)
        let backedUp = Data([0x01])
        let localOnly = Data([0x02])
        let seed = HomeBrowserLocalSeed(
            localIDByFingerprint: [
                backedUp: "remote-twin",
                localOnly: "local-only",
            ],
            assets: [
                HomeBrowserLocalAsset(
                    localIdentifier: "remote-twin",
                    month: january,
                    kind: .photo,
                    creationDateMs: 2_000,
                    fingerprint: backedUp
                ),
                HomeBrowserLocalAsset(
                    localIdentifier: "local-only",
                    month: january,
                    kind: .photo,
                    creationDateMs: 1_000,
                    fingerprint: localOnly
                ),
            ],
            monthGroupingTimeZone: .frozenCurrent()
        )

        let content = try XCTUnwrap(LocalMediaSource.content(
            from: seed,
            backedUpFingerprints: [backedUp],
            excludingBackedUpFingerprints: [backedUp]
        ))
        let sections = content.sections

        XCTAssertEqual(sections.flatMap(\.items).map(\.testLabel), ["local-only"])
        XCTAssertEqual(sections[0].items[0].presence, .localOnly)
        XCTAssertEqual(content.localFingerprintAliases, ["remote-twin": backedUp])
    }

    func testMonthMemoMatchesCanonicalGroupingAcrossBoundaries() throws {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(identifier: "Asia/Kolkata")!
        let dates = [
            try XCTUnwrap(calendar.date(from: DateComponents(year: 2026, month: 3, day: 31))),
            try XCTUnwrap(calendar.date(from: DateComponents(year: 2026, month: 3, day: 1))),
            try XCTUnwrap(calendar.date(from: DateComponents(year: 2026, month: 2, day: 28))),
            try XCTUnwrap(calendar.date(from: DateComponents(year: 2026, month: 2, day: 1))),
        ]
        var memo = MediaBrowserMonthMemo(calendar: calendar)

        for date in dates {
            XCTAssertEqual(
                memo.month(for: date),
                LibraryMonthKey.from(date: date, calendar: calendar)
            )
        }
        XCTAssertEqual(memo.calculationCount, 2)
    }

    func testMissingCreationDateRemainsAbsentForManifests() {
        XCTAssertNil(LibraryCreationDate.optionalMilliseconds(nil))
    }
    func testCreationDateNormalizationPreservesValidDate() {
        let date = Date(timeIntervalSince1970: 1.25)

        let normalized = LibraryCreationDate.normalized(date)

        XCTAssertEqual(normalized.date, date)
        XCTAssertEqual(normalized.milliseconds, 1_250)
    }

    func testCreationDateNormalizationPreservesHistoricalDate() {
        let date = Date(timeIntervalSince1970: -3_786_825_600)

        let normalized = LibraryCreationDate.normalized(date)

        XCTAssertEqual(normalized.date, date)
        XCTAssertEqual(normalized.milliseconds, -3_786_825_600_000)
    }

    func testCreationDateNormalizationRejectsInvalidValues() {
        let candidates: [Date?] = [
            nil,
            Date(timeIntervalSince1970: .nan),
            Date(timeIntervalSince1970: .infinity),
            Date(timeIntervalSince1970: -.infinity),
            Date(timeIntervalSince1970: -1_000_000_000_000),
            Date(timeIntervalSince1970: 1_000_000_000_000)
        ]

        for candidate in candidates {
            let normalized = LibraryCreationDate.normalized(candidate)
            XCTAssertEqual(normalized.date.timeIntervalSince1970, 0)
            XCTAssertEqual(normalized.milliseconds, 0)
        }
    }

    func testCreationDateMillisecondsDoNotCrossMonthBoundary() throws {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0)!
        let marchStart = try XCTUnwrap(calendar.date(from: DateComponents(
            year: 2024,
            month: 3,
            day: 1
        )))
        let candidate = marchStart.addingTimeInterval(-0.0004)

        let normalized = LibraryCreationDate.normalized(candidate)
        let reconstructed = Date(timeIntervalSince1970: Double(normalized.milliseconds) / 1000)

        XCTAssertEqual(LibraryMonthKey.from(date: candidate, calendar: calendar), LibraryMonthKey(year: 2024, month: 2))
        XCTAssertEqual(LibraryMonthKey.from(date: reconstructed, calendar: calendar), LibraryMonthKey(year: 2024, month: 2))
    }

    func testHistoricalCreationDateUsesSharedFlooringRule() throws {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0)!
        let januaryStart = try XCTUnwrap(calendar.date(from: DateComponents(
            year: 1960,
            month: 1,
            day: 1
        )))
        let candidate = januaryStart.addingTimeInterval(-0.0004)

        let normalized = LibraryCreationDate.normalized(candidate)
        let reconstructed = Date(millisecondsSinceEpoch: normalized.milliseconds)

        XCTAssertEqual(normalized.milliseconds, candidate.millisecondsSinceEpoch)
        XCTAssertEqual(LibraryMonthKey.from(date: candidate, calendar: calendar), LibraryMonthKey(year: 1959, month: 12))
        XCTAssertEqual(LibraryMonthKey.from(date: reconstructed, calendar: calendar), LibraryMonthKey(year: 1959, month: 12))
    }
}
