import UIKit

enum PiPLogTailRenderer {
    private static let lineHeight: CGFloat = 13
    private static let font = UIFont.monospacedSystemFont(ofSize: 10, weight: .regular)
    private static let boldFont = UIFont.monospacedSystemFont(ofSize: 10, weight: .semibold)
    private static let secondary = UIColor(white: 1, alpha: 0.55)

    static func draw(entries: [ExecutionLogEntry], in rect: CGRect) {
        guard !entries.isEmpty, rect.height >= lineHeight else { return }

        let maxLines = max(1, Int(rect.height / lineHeight))
        let tail = entries.suffix(maxLines)

        var y = rect.minY
        for entry in tail {
            drawLine(entry, at: CGPoint(x: rect.minX, y: y), maxWidth: rect.width)
            y += lineHeight
        }
    }

    private static func drawLine(_ entry: ExecutionLogEntry, at origin: CGPoint, maxWidth: CGFloat) {
        let timestamp = "[\(ExecutionLogPalette.timestampFormatter.string(from: entry.timestamp))]"
        let tag = "[\(ExecutionLogPalette.tag(for: entry.level))]"
        let color = pipColor(for: entry.level)

        var cursor = origin.x
        let y = origin.y

        let tsSize = (timestamp as NSString).size(withAttributes: [.font: font, .foregroundColor: secondary])
        (timestamp as NSString).draw(
            at: CGPoint(x: cursor, y: y),
            withAttributes: [.font: font, .foregroundColor: secondary]
        )
        cursor += tsSize.width + 4

        let tagSize = (tag as NSString).size(withAttributes: [.font: boldFont, .foregroundColor: color])
        (tag as NSString).draw(
            at: CGPoint(x: cursor, y: y),
            withAttributes: [.font: boldFont, .foregroundColor: color]
        )
        cursor += tagSize.width + 4

        let remainingWidth = max(0, origin.x + maxWidth - cursor)
        let messageRect = CGRect(x: cursor, y: y, width: remainingWidth, height: lineHeight)
        let paragraph = NSMutableParagraphStyle()
        paragraph.lineBreakMode = .byTruncatingTail
        (entry.message as NSString).draw(
            with: messageRect,
            options: [.usesLineFragmentOrigin, .truncatesLastVisibleLine],
            attributes: [.font: font, .foregroundColor: color, .paragraphStyle: paragraph],
            context: nil
        )
    }

    private static func pipColor(for level: ExecutionLogLevel) -> UIColor {
        switch level {
        case .debug: return UIColor(white: 1, alpha: 0.6)
        case .info:  return UIColor(red: 0.55, green: 0.78, blue: 1.0, alpha: 1.0)
        case .warning: return UIColor(red: 1.0, green: 0.72, blue: 0.30, alpha: 1.0)
        case .error: return UIColor(red: 1.0, green: 0.45, blue: 0.45, alpha: 1.0)
        }
    }
}
