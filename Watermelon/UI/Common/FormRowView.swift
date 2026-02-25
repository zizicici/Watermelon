import SnapKit
import UIKit

final class FormRowView: UIView {
    let titleLabel = UILabel()
    let textField = UITextField()

    init(title: String, placeholder: String, isSecure: Bool = false) {
        super.init(frame: .zero)

        titleLabel.font = UIFont.preferredFont(forTextStyle: .subheadline)
        titleLabel.textColor = .secondaryLabel
        titleLabel.text = title

        textField.borderStyle = .roundedRect
        textField.placeholder = placeholder
        textField.isSecureTextEntry = isSecure
        textField.autocapitalizationType = .none
        textField.autocorrectionType = .no

        addSubview(titleLabel)
        addSubview(textField)

        titleLabel.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview()
        }

        textField.snp.makeConstraints { make in
            make.top.equalTo(titleLabel.snp.bottom).offset(6)
            make.leading.trailing.bottom.equalToSuperview()
            make.height.equalTo(36)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}
