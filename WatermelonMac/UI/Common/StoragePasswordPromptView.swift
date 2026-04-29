import SwiftUI

struct StoragePasswordPromptView: View {
    let profileName: String
    let username: String
    let onSubmit: (String) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var password: String = ""

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text(String(localized: "migration.password.title")).font(.headline)
            Text(String(format: String(localized: "migration.password.message"), profileName, username))
                .font(.callout)
                .foregroundStyle(.secondary)

            SecureField(String(localized: "migration.password.field"), text: $password)
                .textFieldStyle(.roundedBorder)
                .onSubmit { submit() }

            HStack {
                Spacer()
                Button(String(localized: "common.cancel")) { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button(String(localized: "common.connect")) { submit() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(password.isEmpty)
            }
        }
        .padding(20)
        .frame(width: 400)
    }

    private func submit() {
        let value = password
        onSubmit(value)
        dismiss()
    }
}
