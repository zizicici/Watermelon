import Foundation

enum MacDownloadLocalFingerprintPolicy {
    static func freshRecords(
        snapshots: some Sequence<LibraryAssetSnapshot>,
        records: [String: LocalAssetFingerprintRecord]
    ) -> [String: LocalAssetFingerprintRecord] {
        LocalAssetFingerprintFreshness.evaluate(
            snapshots: snapshots,
            records: records
        ).freshRecords
    }

    static func freshFingerprints(
        snapshots: some Sequence<LibraryAssetSnapshot>,
        records: [String: LocalAssetFingerprintRecord]
    ) -> Set<Data> {
        Set(
            freshRecords(
                snapshots: snapshots,
                records: records
            ).values.map(\.fingerprint)
        )
    }
}
