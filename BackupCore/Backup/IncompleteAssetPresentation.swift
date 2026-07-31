import Foundation

struct IncompleteAssetSection: Sendable {
    let month: LibraryMonthKey
    let entries: [IncompleteAssetEntry]
}

enum IncompleteAssetPresentation {
    static func sections(
        from entries: [IncompleteAssetEntry]
    ) -> [IncompleteAssetSection] {
        var grouped: [LibraryMonthKey: [IncompleteAssetEntry]] = [:]
        for entry in entries {
            grouped[entry.month, default: []].append(entry)
        }
        return grouped.keys.sorted(by: >).map { month in
            let monthEntries = (grouped[month] ?? []).sorted {
                lhs,
                rhs in
                let leftDate = lhs.creationDate ?? .distantPast
                let rightDate = rhs.creationDate ?? .distantPast
                if leftDate != rightDate {
                    return leftDate < rightDate
                }
                return lhs.id.lexicographicallyPrecedes(rhs.id)
            }
            return IncompleteAssetSection(
                month: month,
                entries: monthEntries
            )
        }
    }

    static func fileName(for entry: IncompleteAssetEntry) -> String {
        entry.representativeFileName
            ?? String(
                localized:
                    "storage.detail.incompleteAssets.unknownFileName"
            )
    }

    static func summary(for entry: IncompleteAssetEntry) -> String {
        let missingText = String.localizedStringWithFormat(
            String(
                localized:
                    "storage.detail.incompleteAssets.missingCount"
            ),
            entry.missingResourceCount,
            entry.totalResourceCount
        )
        let dateText = entry.creationDate?.formatted(
            date: .abbreviated,
            time: .shortened
        ) ?? "-"
        return "\(missingText) · \(dateText) · "
            + String(entry.id.hexString.prefix(16))
    }

    static func detailText(
        for entry: IncompleteAssetEntry
    ) -> String {
        var lines = [
            String.localizedStringWithFormat(
                String(
                    localized:
                        "storage.detail.incompleteAssets.fingerprintLine"
                ),
                entry.id.hexString
            )
        ]
        if let name = entry.representativeFileName {
            lines.append(
                String.localizedStringWithFormat(
                    String(
                        localized:
                            "storage.detail.incompleteAssets.fileLine"
                    ),
                    name
                )
            )
        }
        if let date = entry.creationDate {
            lines.append(
                String.localizedStringWithFormat(
                    String(
                        localized:
                            "storage.detail.incompleteAssets.createdLine"
                    ),
                    date.formatted(
                        date: .abbreviated,
                        time: .shortened
                    )
                )
            )
        }
        if entry.missingResourceHashes.isEmpty {
            lines.append(
                String(
                    localized:
                        "storage.detail.incompleteAssets.noMissingHashes"
                )
            )
        } else {
            lines.append(
                String(
                    localized:
                        "storage.detail.incompleteAssets.missingHashesHeader"
                )
            )
            lines.append(
                contentsOf: entry.missingResourceHashes.map {
                    "  \(String($0.hexString.prefix(32)))"
                }
            )
        }
        return lines.joined(separator: "\n")
    }
}
