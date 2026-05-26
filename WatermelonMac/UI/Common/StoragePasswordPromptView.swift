import SwiftUI

struct StoragePasswordPromptView: View {
    let profileName: String
    let username: String
    let storageType: StorageType
    let onSubmit: (String) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var password: String = ""

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text(title).font(.headline)
            Text(String(format: message, profileName, username))
                .font(.callout)
                .foregroundStyle(.secondary)

            SecureField(fieldLabel, text: $password)
                .textFieldStyle(.roundedBorder)
                .onSubmit { submit() }

            HStack {
                Spacer()
                Button(String(localized: "common.cancel")) { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button(String(localized: "common.connect")) { submit() }
                    .keyboardShortcut(.defaultAction)
            }
        }
        .padding(20)
        .frame(width: 400)
    }

    private var title: String {
        storageType == .s3
            ? String(localized: "migration.secretKey.title")
            : String(localized: "migration.password.title")
    }

    private var message: String {
        storageType == .s3
            ? String(localized: "migration.secretKey.message")
            : String(localized: "migration.password.message")
    }

    private var fieldLabel: String {
        storageType == .s3
            ? String(localized: "profile.add.s3.secretKey")
            : String(localized: "migration.password.field")
    }

    private func submit() {
        let value = password
        onSubmit(value)
        dismiss()
    }
}
