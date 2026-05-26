import LocalAuthentication
import SnapKit
import UIKit

enum CredentialMask {
    static let saved = "********"
    static let trailingInset: CGFloat = 16
    static let revealButtonSize = CGSize(width: 32, height: 32)
    static let revealSymbolConfiguration = UIImage.SymbolConfiguration(pointSize: 14, weight: .regular)
    static let fieldFont = UIFont.monospacedSystemFont(ofSize: UIFont.preferredFont(forTextStyle: .body).pointSize, weight: .regular)
    static let textViewFont = UIFont.monospacedSystemFont(ofSize: 12, weight: .regular)

    static func revealAccessibilityLabel(isRevealed: Bool, revealLabel: String, hideLabel: String) -> String {
        isRevealed ? hideLabel : revealLabel
    }
}

enum CredentialRevealAuthenticator {
    @MainActor
    static func authenticate(localizedReason: String) async -> Bool {
        let context = LAContext()
        var error: NSError?
        guard context.canEvaluatePolicy(.deviceOwnerAuthentication, error: &error) else {
            return false
        }
        do {
            return try await context.evaluatePolicy(
                .deviceOwnerAuthentication,
                localizedReason: localizedReason
            )
        } catch {
            return false
        }
    }
}

final class SettingsTextFieldCell: UITableViewCell, UITextFieldDelegate {
    static let reuseIdentifier = "SettingsTextFieldCell"

    private let titleLabel = UILabel()
    let textField = UITextField()
    private lazy var focusTapGestureRecognizer: UITapGestureRecognizer = {
        let gesture = UITapGestureRecognizer(target: self, action: #selector(handleCellTap))
        gesture.cancelsTouchesInView = false
        return gesture
    }()

    private var textFieldLeadingToTitleConstraint: Constraint?
    private var textFieldLeadingToSuperviewConstraint: Constraint?

    var onTextChanged: ((String) -> Void)?
    var onReturn: (() -> Void)?

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)

        selectionStyle = .none

        let background: UIBackgroundConfiguration
        if #available(iOS 18.0, *) {
            background = .listCell()
        } else {
            background = .listGroupedCell()
        }
        var configuredBackground = background
        configuredBackground.backgroundColor = .secondarySystemGroupedBackground
        backgroundConfiguration = configuredBackground

        titleLabel.font = .preferredFont(forTextStyle: .body)
        titleLabel.textColor = .label
        titleLabel.setContentHuggingPriority(.required, for: .horizontal)
        titleLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        textField.borderStyle = .none
        textField.textAlignment = .right
        textField.font = .preferredFont(forTextStyle: .body)
        textField.textColor = .label
        textField.clearButtonMode = .whileEditing
        textField.autocorrectionType = .no
        textField.autocapitalizationType = .none
        textField.returnKeyType = .next
        textField.delegate = self
        textField.addTarget(self, action: #selector(textDidChange), for: .editingChanged)

        contentView.addSubview(titleLabel)
        contentView.addSubview(textField)
        contentView.addGestureRecognizer(focusTapGestureRecognizer)

        titleLabel.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(16)
            make.centerY.equalToSuperview()
        }

        textField.snp.makeConstraints { make in
            self.textFieldLeadingToTitleConstraint = make.leading.greaterThanOrEqualTo(titleLabel.snp.trailing).offset(12).constraint
            self.textFieldLeadingToSuperviewConstraint = make.leading.equalToSuperview().inset(16).constraint
            make.trailing.equalToSuperview().inset(16)
            make.top.bottom.equalToSuperview().inset(8)
            make.height.greaterThanOrEqualTo(36)
        }

        textFieldLeadingToSuperviewConstraint?.deactivate()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        onTextChanged = nil
        onReturn = nil
        textField.inputAccessoryView = nil
    }

    func configure(
        title: String?,
        text: String,
        placeholder: String,
        isSecure: Bool = false,
        keyboardType: UIKeyboardType = .default,
        autocapitalizationType: UITextAutocapitalizationType = .none,
        returnKeyType: UIReturnKeyType = .next,
        inputAccessoryView: UIView? = nil
    ) {
        let hasTitle = !(title?.isEmpty ?? true)
        titleLabel.text = title
        titleLabel.isHidden = !hasTitle
        textField.text = text
        textField.placeholder = placeholder
        textField.isSecureTextEntry = isSecure
        textField.keyboardType = keyboardType
        textField.autocapitalizationType = autocapitalizationType
        textField.returnKeyType = returnKeyType
        textField.inputAccessoryView = inputAccessoryView
        textField.textAlignment = hasTitle ? .right : .left
        isUserInteractionEnabled = true
        contentView.alpha = 1

        if hasTitle {
            textFieldLeadingToSuperviewConstraint?.deactivate()
            textFieldLeadingToTitleConstraint?.activate()
        } else {
            textFieldLeadingToTitleConstraint?.deactivate()
            textFieldLeadingToSuperviewConstraint?.activate()
        }
    }

    func focus() {
        textField.becomeFirstResponder()
    }

    @objc
    private func textDidChange() {
        onTextChanged?(textField.text ?? "")
    }

    @objc
    private func handleCellTap() {
        guard !textField.isFirstResponder else { return }
        textField.becomeFirstResponder()
    }

    func textFieldShouldReturn(_ textField: UITextField) -> Bool {
        onReturn?()
        return false
    }
}

final class CredentialTextFieldCell: UITableViewCell, UITextFieldDelegate {
    static let reuseIdentifier = "CredentialTextFieldCell"

    private let titleLabel = UILabel()
    let textField = UITextField()
    private let revealButton = UIButton(type: .system)

    private var isShowingMask = false
    private var isRevealed = false

    var onTextChanged: ((String) -> Void)?
    var onRevealTapped: (() -> Void)?
    var onMaskedCredentialEdited: ((String) -> Void)?
    var onEndEditing: (() -> Void)?
    var onReturn: (() -> Void)?

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)

        selectionStyle = .none

        var background: UIBackgroundConfiguration
        if #available(iOS 18.0, *) {
            background = .listCell()
        } else {
            background = .listGroupedCell()
        }
        background.backgroundColor = .secondarySystemGroupedBackground
        backgroundConfiguration = background

        titleLabel.font = .preferredFont(forTextStyle: .body)
        titleLabel.textColor = .label
        titleLabel.setContentHuggingPriority(.required, for: .horizontal)
        titleLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        revealButton.tintColor = .secondaryLabel
        revealButton.addTarget(self, action: #selector(revealTapped), for: .touchUpInside)

        textField.borderStyle = .none
        textField.textAlignment = .right
        textField.font = CredentialMask.fieldFont
        textField.textColor = .label
        textField.autocorrectionType = .no
        textField.autocapitalizationType = .none
        textField.clearButtonMode = .never
        textField.delegate = self
        textField.addTarget(self, action: #selector(textDidChange), for: .editingChanged)

        contentView.addSubview(titleLabel)
        contentView.addSubview(textField)
        contentView.addSubview(revealButton)

        titleLabel.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(16)
            make.centerY.equalToSuperview()
        }

        revealButton.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(CredentialMask.trailingInset)
            make.centerY.equalTo(textField)
            make.size.equalTo(CredentialMask.revealButtonSize)
        }

        textField.snp.makeConstraints { make in
            make.leading.greaterThanOrEqualTo(titleLabel.snp.trailing).offset(12)
            make.trailing.equalTo(revealButton.snp.leading).offset(-8)
            make.top.bottom.equalToSuperview().inset(8)
            make.height.greaterThanOrEqualTo(36)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        onTextChanged = nil
        onRevealTapped = nil
        onMaskedCredentialEdited = nil
        onEndEditing = nil
        onReturn = nil
        textField.inputAccessoryView = nil
        isShowingMask = false
        isRevealed = false
    }

    func configure(
        title: String,
        text: String,
        placeholder: String,
        isMasked: Bool,
        isRevealed: Bool,
        revealAccessibilityLabel: String,
        hideAccessibilityLabel: String,
        inputAccessoryView: UIView? = nil
    ) {
        titleLabel.text = title
        textField.placeholder = placeholder
        textField.inputAccessoryView = inputAccessoryView
        applyCredentialTextStyle(isSecure: !isMasked && !isRevealed)
        textField.text = isMasked ? CredentialMask.saved : text
        isShowingMask = isMasked
        self.isRevealed = isRevealed
        let imageName = isRevealed ? "eye.slash" : "eye"
        let image = UIImage(systemName: imageName, withConfiguration: CredentialMask.revealSymbolConfiguration)
        revealButton.setImage(image, for: .normal)
        revealButton.accessibilityLabel = CredentialMask.revealAccessibilityLabel(
            isRevealed: isRevealed,
            revealLabel: revealAccessibilityLabel,
            hideLabel: hideAccessibilityLabel
        )
    }

    func focus() {
        textField.becomeFirstResponder()
    }

    @objc
    private func textDidChange() {
        guard !isShowingMask else { return }
        onTextChanged?(textField.text ?? "")
    }

    @objc
    private func revealTapped() {
        onRevealTapped?()
    }

    private func applyCredentialTextStyle(isSecure: Bool) {
        textField.isSecureTextEntry = isSecure
        textField.font = CredentialMask.fieldFont
        textField.defaultTextAttributes = [
            .font: CredentialMask.fieldFont,
            .foregroundColor: UIColor.label
        ]
        textField.textColor = .label
    }

    func textField(
        _ textField: UITextField,
        shouldChangeCharactersIn _: NSRange,
        replacementString string: String
    ) -> Bool {
        guard isShowingMask else { return true }
        guard !string.isEmpty else { return false }
        isShowingMask = false
        applyCredentialTextStyle(isSecure: !isRevealed)
        textField.text = string
        onMaskedCredentialEdited?(textField.text ?? "")
        return false
    }

    func textFieldDidEndEditing(_ textField: UITextField) {
        onEndEditing?()
    }

    func textFieldShouldReturn(_ textField: UITextField) -> Bool {
        onReturn?()
        return false
    }
}

final class CredentialTextViewCell: UITableViewCell, UITextViewDelegate {
    static let reuseIdentifier = "CredentialTextViewCell"

    private let titleLabel = UILabel()
    private let revealButton = UIButton(type: .system)
    private let textView = UITextView()
    private let placeholderLabel = UILabel()

    private var isShowingMask = false
    private var isHidingEnteredText = false
    private var currentRevealAccessibilityLabel = ""
    private var currentHideAccessibilityLabel = ""

    var onTextChanged: ((String) -> Void)?
    var onMaskedCredentialEdited: ((String) -> Void)?
    var onRevealTapped: (() -> Void)?

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)
        selectionStyle = .none

        var background: UIBackgroundConfiguration
        if #available(iOS 18.0, *) {
            background = .listCell()
        } else {
            background = .listGroupedCell()
        }
        background.backgroundColor = .secondarySystemGroupedBackground
        backgroundConfiguration = background

        titleLabel.font = .preferredFont(forTextStyle: .body)
        titleLabel.textColor = .label

        revealButton.tintColor = .secondaryLabel
        revealButton.addTarget(self, action: #selector(revealTapped), for: .touchUpInside)

        textView.font = CredentialMask.textViewFont
        textView.backgroundColor = .clear
        textView.autocapitalizationType = .none
        textView.autocorrectionType = .no
        textView.spellCheckingType = .no
        textView.smartQuotesType = .no
        textView.smartDashesType = .no
        textView.delegate = self
        textView.isScrollEnabled = false
        textView.textContainerInset = .zero
        textView.textContainer.lineFragmentPadding = 0

        placeholderLabel.font = textView.font
        placeholderLabel.textColor = .placeholderText
        placeholderLabel.numberOfLines = 0

        contentView.addSubview(titleLabel)
        contentView.addSubview(revealButton)
        contentView.addSubview(textView)
        contentView.addSubview(placeholderLabel)

        titleLabel.snp.makeConstraints { make in
            make.top.leading.equalToSuperview().inset(16)
            make.trailing.lessThanOrEqualTo(revealButton.snp.leading).offset(-8)
        }
        revealButton.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(CredentialMask.trailingInset)
            make.centerY.equalTo(titleLabel)
            make.size.equalTo(CredentialMask.revealButtonSize)
        }
        textView.snp.makeConstraints { make in
            make.top.equalTo(titleLabel.snp.bottom).offset(8)
            make.leading.trailing.bottom.equalToSuperview().inset(16)
            make.height.greaterThanOrEqualTo(120)
        }
        placeholderLabel.snp.makeConstraints { make in
            make.top.equalTo(textView)
            make.leading.trailing.equalTo(textView)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        onTextChanged = nil
        onMaskedCredentialEdited = nil
        onRevealTapped = nil
        textView.text = ""
        textView.isEditable = true
        placeholderLabel.isHidden = false
        isShowingMask = false
        isHidingEnteredText = false
        currentRevealAccessibilityLabel = ""
        currentHideAccessibilityLabel = ""
    }

    func configure(
        title: String,
        placeholder: String,
        text: String,
        isMasked: Bool,
        isRevealed: Bool,
        hidesEnteredText: Bool,
        revealAccessibilityLabel: String,
        hideAccessibilityLabel: String
    ) {
        titleLabel.text = title
        placeholderLabel.text = placeholder
        currentRevealAccessibilityLabel = revealAccessibilityLabel
        currentHideAccessibilityLabel = hideAccessibilityLabel
        isHidingEnteredText = hidesEnteredText && !isMasked && !isRevealed && !text.isEmpty
        let displayText = (isMasked || isHidingEnteredText) ? CredentialMask.saved : text
        textView.text = displayText
        textView.font = CredentialMask.textViewFont
        placeholderLabel.font = CredentialMask.textViewFont
        textView.isEditable = !isHidingEnteredText
        placeholderLabel.isHidden = !displayText.isEmpty
        isShowingMask = isMasked
        setRevealButton(isRevealed: isRevealed)
    }

    @objc
    private func revealTapped() {
        onRevealTapped?()
    }

    private func setRevealButton(isRevealed: Bool) {
        let imageName = isRevealed ? "eye.slash" : "eye"
        let image = UIImage(systemName: imageName, withConfiguration: CredentialMask.revealSymbolConfiguration)
        revealButton.setImage(image, for: .normal)
        revealButton.accessibilityLabel = CredentialMask.revealAccessibilityLabel(
            isRevealed: isRevealed,
            revealLabel: currentRevealAccessibilityLabel,
            hideLabel: currentHideAccessibilityLabel
        )
    }

    func textView(
        _ textView: UITextView,
        shouldChangeTextIn _: NSRange,
        replacementText text: String
    ) -> Bool {
        guard isShowingMask else { return true }
        guard !text.isEmpty else { return false }
        isShowingMask = false
        isHidingEnteredText = false
        textView.font = CredentialMask.textViewFont
        textView.text = text
        placeholderLabel.isHidden = !textView.text.isEmpty
        setRevealButton(isRevealed: true)
        onMaskedCredentialEdited?(textView.text)
        return false
    }

    func textViewDidChange(_ textView: UITextView) {
        isShowingMask = false
        isHidingEnteredText = false
        placeholderLabel.isHidden = !textView.text.isEmpty
        setRevealButton(isRevealed: true)
        onTextChanged?(textView.text)
    }
}
