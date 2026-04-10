import UIKit

// MARK: - Month Summary

struct HomeMonthSummary {
    let month: LibraryMonthKey
    let assetCount: Int
    let photoCount: Int
    let videoCount: Int
    let backedUpCount: Int?
    let totalSizeBytes: Int64?

    var monthTitle: String {
        String(format: "%02d月", month.month)
    }

    func countAttributedText(color: UIColor) -> NSAttributedString {
        let font = UIFont.monospacedDigitSystemFont(ofSize: 13, weight: .regular)
        let symbolConfig = UIImage.SymbolConfiguration(pointSize: 10, weight: .bold)
        let result = NSMutableAttributedString()

        if let img = UIImage(systemName: "photo", withConfiguration: symbolConfig)?.withTintColor(color, renderingMode: .alwaysOriginal) {
            result.append(NSAttributedString(attachment: NSTextAttachment(image: img)))
        }
        result.append(NSAttributedString(string: " \(photoCount)  ", attributes: [.font: font, .foregroundColor: color]))

        if let img = UIImage(systemName: "video", withConfiguration: symbolConfig)?.withTintColor(color, renderingMode: .alwaysOriginal) {
            result.append(NSAttributedString(attachment: NSTextAttachment(image: img)))
        }
        result.append(NSAttributedString(string: " \(videoCount)", attributes: [.font: font, .foregroundColor: color]))

        return result
    }

    var sizeText: String? {
        guard let bytes = totalSizeBytes else { return nil }
        return ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
    }
}

// MARK: - Month Row

struct HomeMonthRow: Equatable {
    let month: LibraryMonthKey
    var local: HomeMonthSummary?
    var remote: HomeMonthSummary?

    static func == (lhs: HomeMonthRow, rhs: HomeMonthRow) -> Bool { lhs.month == rhs.month }
}

// MARK: - Year Section

struct HomeMergedYearSection {
    let year: Int
    let rows: [HomeMonthRow]

    var title: String { "\(year)年" }

    var localPhotoCount: Int { rows.compactMap(\.local).reduce(0) { $0 + $1.photoCount } }
    var localVideoCount: Int { rows.compactMap(\.local).reduce(0) { $0 + $1.videoCount } }
    var remotePhotoCount: Int { rows.compactMap(\.remote).reduce(0) { $0 + $1.photoCount } }
    var remoteVideoCount: Int { rows.compactMap(\.remote).reduce(0) { $0 + $1.videoCount } }

    var localSizeBytes: Int64? {
        let sizes = rows.compactMap { $0.local?.totalSizeBytes }
        let locals = rows.compactMap(\.local)
        guard !locals.isEmpty, sizes.count == locals.count else { return nil }
        return sizes.reduce(0, +)
    }

    var remoteSizeBytes: Int64? {
        let sizes = rows.compactMap { $0.remote?.totalSizeBytes }
        let remotes = rows.compactMap(\.remote)
        guard !remotes.isEmpty, sizes.count == remotes.count else { return nil }
        return sizes.reduce(0, +)
    }
}

// MARK: - Selection & Direction

enum HomeSelectionState {
    case none, partial, all
}

enum HomeArrowDirection {
    case toRemote      // arrow.right
    case toLocal       // arrow.left
    case sync          // arrow.left.arrow.right
}

// MARK: - Season Styling

enum HomeSeasonStyle {
    private struct Style {
        let bg: UIColor
        let title: UIColor
        let detail: UIColor
    }

    private static let styles: [Style] = [
        Style(
            bg:     .materialSurface(light: .Material.Green._50, darkTint: .Material.Green._200),
            title:  .materialOnContainer(light: .Material.Green._900, dark: .Material.Green._100),
            detail: .materialOnSurfaceVariant(light: .Material.Green._700, dark: .Material.Green._200)
        ),
        Style(
            bg:     .materialSurface(light: .Material.Blue._50, darkTint: .Material.Blue._200),
            title:  .materialOnContainer(light: .Material.Blue._900, dark: .Material.Blue._100),
            detail: .materialOnSurfaceVariant(light: .Material.Blue._700, dark: .Material.Blue._200)
        ),
        Style(
            bg:     .materialSurface(light: .Material.Amber._50, darkTint: .Material.Amber._200),
            title:  .materialOnContainer(light: .Material.Amber._900, dark: .Material.Amber._100),
            detail: .materialOnSurfaceVariant(light: .Material.Amber._700, dark: .Material.Amber._200)
        ),
        Style(
            bg:     .materialSurface(light: .Material.Red._50, darkTint: .Material.Red._200),
            title:  .materialOnContainer(light: .Material.Red._900, dark: .Material.Red._100),
            detail: .materialOnSurfaceVariant(light: .Material.Red._700, dark: .Material.Red._200)
        ),
    ]

    static func seasonIndex(for month: Int) -> Int {
        switch month {
        case 1...3:  return 0
        case 4...6:  return 1
        case 7...9:  return 2
        case 10...12: return 3
        default:      return 0
        }
    }

    static func monthColor(month: Int) -> UIColor { styles[seasonIndex(for: month)].bg }
    static func monthTextColor(month: Int) -> UIColor { styles[seasonIndex(for: month)].title }
    static func monthSecondaryTextColor(month: Int) -> UIColor { styles[seasonIndex(for: month)].detail }
}
