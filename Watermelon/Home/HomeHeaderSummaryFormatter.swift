import UIKit

struct HomeHeaderSummary {
    let photoCount: Int
    let videoCount: Int
    let totalSizeBytes: Int64?
}

@MainActor
enum HomeHeaderSummaryFormatter {
    /// Aggregate photo/video/size totals across all month rows for one side.
    /// `treatsEmptyAsZero` returns a zero-filled summary when there's nothing to
    /// aggregate; otherwise returns nil so the caller can render a placeholder.
    static func aggregate(
        rowLookup: [LibraryMonthKey: HomeMonthRow],
        side: SelectionSide,
        treatsEmptyAsZero: Bool
    ) -> HomeHeaderSummary? {
        let summaries = rowLookup.values.compactMap { row in
            switch side {
            case .local: return row.local
            case .remote: return row.remote
            }
        }

        guard !summaries.isEmpty else {
            guard treatsEmptyAsZero else { return nil }
            return HomeHeaderSummary(photoCount: 0, videoCount: 0, totalSizeBytes: 0)
        }

        let totalPhotoCount = summaries.reduce(0) { $0 + $1.photoCount }
        let totalVideoCount = summaries.reduce(0) { $0 + $1.videoCount }
        let sizeValues = summaries.compactMap(\.totalSizeBytes)
        // Only emit a total size when every summary has one — partial coverage would
        // mislead the UI into showing an undercount.
        let totalSizeBytes = sizeValues.count == summaries.count ? sizeValues.reduce(0, +) : nil

        return HomeHeaderSummary(
            photoCount: totalPhotoCount,
            videoCount: totalVideoCount,
            totalSizeBytes: totalSizeBytes
        )
    }

    static func apply(
        _ summary: HomeHeaderSummary,
        countLabel: UILabel,
        sizeLabel: UILabel,
        color: UIColor
    ) {
        countLabel.text = nil
        countLabel.attributedText = makeCountText(
            photoCount: summary.photoCount,
            videoCount: summary.videoCount,
            color: color
        )
        if let totalSizeBytes = summary.totalSizeBytes {
            sizeLabel.attributedText = nil
            sizeLabel.text = ByteCountFormatter.string(fromByteCount: totalSizeBytes, countStyle: .file)
        } else {
            sizeLabel.attributedText = nil
            sizeLabel.text = "-"
        }
    }

    static func applyPlaceholder(countLabel: UILabel, sizeLabel: UILabel) {
        countLabel.attributedText = nil
        countLabel.text = "-"
        sizeLabel.attributedText = nil
        sizeLabel.text = "-"
    }

    static func makeCountText(photoCount: Int, videoCount: Int, color: UIColor) -> NSAttributedString {
        let font = UIFont.monospacedDigitSystemFont(ofSize: 12, weight: .regular)
        let symbolConfig = UIImage.SymbolConfiguration(pointSize: 9, weight: .bold)
        let result = NSMutableAttributedString()

        if let image = UIImage(systemName: "photo", withConfiguration: symbolConfig)?
            .withTintColor(color, renderingMode: .alwaysOriginal) {
            result.append(NSAttributedString(attachment: NSTextAttachment(image: image)))
        }
        result.append(NSAttributedString(
            string: " \(photoCount)  ",
            attributes: [.font: font, .foregroundColor: color]
        ))

        if let image = UIImage(systemName: "video", withConfiguration: symbolConfig)?
            .withTintColor(color, renderingMode: .alwaysOriginal) {
            result.append(NSAttributedString(attachment: NSTextAttachment(image: image)))
        }
        result.append(NSAttributedString(
            string: " \(videoCount)",
            attributes: [.font: font, .foregroundColor: color]
        ))

        return result
    }
}
