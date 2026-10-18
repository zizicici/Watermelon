import UIKit

final class WatermelonFeatureInfoViewController: UIViewController {
    let message: String
    private let headingFont = UIFontMetrics(forTextStyle: .subheadline).scaledFont(for: .systemFont(ofSize: 15, weight: .semibold))
    private let bodyFont = UIFontMetrics(forTextStyle: .footnote).scaledFont(for: .systemFont(ofSize: 14))
    private let paragraph = NSMutableParagraphStyle()

    init(title: String, message: String, width: CGFloat) {
        self.message = message
        super.init(nibName: nil, bundle: nil)
        self.title = title
        paragraph.lineSpacing = 3
        paragraph.paragraphSpacing = 8
        let height = (message as NSString).boundingRect(
            with: CGSize(width: width - 36, height: .greatestFiniteMagnitude),
            options: [.usesLineFragmentOrigin, .usesFontLeading],
            attributes: [.font: bodyFont, .paragraphStyle: paragraph], context: nil
        ).height
        let titleHeight = (title as NSString).boundingRect(
            with: CGSize(width: width - 36, height: .greatestFiniteMagnitude),
            options: [.usesLineFragmentOrigin, .usesFontLeading],
            attributes: [.font: headingFont], context: nil
        ).height
        preferredContentSize = CGSize(width: width, height: min(420, ceil(height + titleHeight) + 44))
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        let heading = UILabel()
        heading.text = title
        heading.font = headingFont
        heading.adjustsFontForContentSizeCategory = true
        heading.numberOfLines = 0
        let body = UILabel()
        body.font = bodyFont
        body.adjustsFontForContentSizeCategory = true
        body.textColor = .label
        body.attributedText = NSAttributedString(string: message, attributes: [.paragraphStyle: paragraph])
        body.numberOfLines = 0
        let scroll = UIScrollView()
        let content = UIStackView(arrangedSubviews: [heading, body])
        content.axis = .vertical
        content.spacing = 10
        scroll.translatesAutoresizingMaskIntoConstraints = false
        content.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(scroll)
        scroll.addSubview(content)
        NSLayoutConstraint.activate([
            scroll.topAnchor.constraint(equalTo: view.topAnchor),
            scroll.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            scroll.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            scroll.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            content.topAnchor.constraint(equalTo: scroll.contentLayoutGuide.topAnchor, constant: 16),
            content.bottomAnchor.constraint(equalTo: scroll.contentLayoutGuide.bottomAnchor, constant: -18),
            content.leadingAnchor.constraint(equalTo: scroll.contentLayoutGuide.leadingAnchor, constant: 18),
            content.trailingAnchor.constraint(equalTo: scroll.contentLayoutGuide.trailingAnchor, constant: -18),
            content.widthAnchor.constraint(equalTo: scroll.frameLayoutGuide.widthAnchor, constant: -36)
        ])
    }
}
