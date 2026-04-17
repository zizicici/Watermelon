import SnapKit
import UIKit

final class SettingsTextFieldCell: UITableViewCell, UITextFieldDelegate {
    static let reuseIdentifier = "SettingsTextFieldCell"

    private let titleLabel = UILabel()
    let textField = UITextField()

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
        textField.textColor = .secondaryLabel
        textField.clearButtonMode = .whileEditing
        textField.autocorrectionType = .no
        textField.autocapitalizationType = .none
        textField.returnKeyType = .next
        textField.delegate = self
        textField.addTarget(self, action: #selector(textDidChange), for: .editingChanged)

        contentView.addSubview(titleLabel)
        contentView.addSubview(textField)

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

    func textFieldShouldReturn(_ textField: UITextField) -> Bool {
        onReturn?()
        return false
    }
}
