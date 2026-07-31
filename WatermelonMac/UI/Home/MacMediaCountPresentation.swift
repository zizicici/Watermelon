import AppKit

struct MacMediaCountItem: Equatable, Sendable {
    let symbolName: String
    let count: Int
}

enum MacMediaCountPresentation {
    static func items(
        photoCount: Int,
        videoCount: Int
    ) -> [MacMediaCountItem] {
        [
            MacMediaCountItem(
                symbolName: "photo",
                count: photoCount
            ),
            MacMediaCountItem(
                symbolName: "video",
                count: videoCount
            ),
        ]
    }

    @MainActor
    static func attributedString(
        photoCount: Int,
        videoCount: Int,
        fontSize: CGFloat,
        color: NSColor
    ) -> NSAttributedString {
        let font = NSFont.monospacedDigitSystemFont(
            ofSize: fontSize,
            weight: .regular
        )
        let symbolConfiguration = NSImage.SymbolConfiguration(
            pointSize: max(fontSize - 2, 8),
            weight: .semibold
        )
        let result = NSMutableAttributedString()

        for (index, item) in items(
            photoCount: photoCount,
            videoCount: videoCount
        ).enumerated() {
            if index > 0 {
                result.append(NSAttributedString(string: "  "))
            }
            if let image = NSImage(
                systemSymbolName: item.symbolName,
                accessibilityDescription: nil
            )?.withSymbolConfiguration(symbolConfiguration) {
                let attachment = NSTextAttachment()
                attachment.image = image
                result.append(NSAttributedString(attachment: attachment))
            }
            result.append(
                NSAttributedString(
                    string: " \(item.count)",
                    attributes: [
                        .font: font,
                        .foregroundColor: color,
                    ]
                )
            )
        }

        return result
    }

    @MainActor
    static func summaryAttributedString(
        photoCount: Int,
        videoCount: Int,
        sizeBytes: Int64?,
        fontSize: CGFloat,
        color: NSColor
    ) -> NSAttributedString {
        let result = NSMutableAttributedString(
            attributedString: attributedString(
                photoCount: photoCount,
                videoCount: videoCount,
                fontSize: fontSize,
                color: color
            )
        )
        guard let sizeBytes else { return result }
        result.append(
            NSAttributedString(
                string: "  ·  " + ByteCountFormatter.string(
                    fromByteCount: sizeBytes,
                    countStyle: .file
                ),
                attributes: [
                    .font: NSFont.monospacedDigitSystemFont(
                        ofSize: fontSize,
                        weight: .regular
                    ),
                    .foregroundColor: color,
                ]
            )
        )
        return result
    }
}
