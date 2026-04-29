import SwiftUI

struct AddSMBProfileSheet: View {
    @ObservedObject var store: ProfileStore
    let onSaved: (ServerProfileRecord) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var name: String = ""
    @State private var host: String = ""
    @State private var portString: String = "445"
    @State private var shareName: String = ""
    @State private var basePath: String = "/Watermelon"
    @State private var username: String = ""
    @State private var domain: String = ""
    @State private var password: String = ""
    @State private var saveError: String?
    @State private var saving = false

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Add SMB Profile").font(.headline)

            Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 10) {
                row("Name", placeholder: "Optional — defaults to host", text: $name)
                row("Host", placeholder: "e.g. 192.168.1.10", text: $host)
                row("Port", placeholder: "445", text: $portString)
                row("Share", placeholder: "e.g. backup", text: $shareName)
                row("Base path", placeholder: "/Watermelon", text: $basePath)
                row("Username", placeholder: "", text: $username)
                row("Domain", placeholder: "Optional", text: $domain)
                GridRow {
                    Text("Password").gridColumnAlignment(.trailing).foregroundStyle(.secondary)
                    SecureField("", text: $password)
                        .textFieldStyle(.roundedBorder)
                }
            }

            if let saveError {
                Text(saveError).font(.callout).foregroundStyle(.red)
            }

            HStack {
                Spacer()
                Button("Cancel") { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button(saving ? "Saving…" : "Save") { save() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(!isValid || saving)
            }
        }
        .padding(20)
        .frame(width: 480)
    }

    @ViewBuilder
    private func row(_ label: String, placeholder: String, text: Binding<String>) -> some View {
        GridRow {
            Text(label).gridColumnAlignment(.trailing).foregroundStyle(.secondary)
            TextField(placeholder, text: text)
                .textFieldStyle(.roundedBorder)
        }
    }

    private var isValid: Bool {
        !host.trimmed.isEmpty &&
        !shareName.trimmed.isEmpty &&
        !username.trimmed.isEmpty &&
        !password.isEmpty &&
        Int(portString) != nil
    }

    private func save() {
        saving = true
        defer { saving = false }
        guard let port = Int(portString) else {
            saveError = "Port must be a number"
            return
        }
        do {
            let record = try store.saveSMBProfile(
                name: name,
                host: host.trimmed,
                port: port,
                shareName: shareName.trimmed,
                basePath: basePath.trimmed.isEmpty ? "/" : basePath.trimmed,
                username: username.trimmed,
                domain: domain.trimmed.isEmpty ? nil : domain.trimmed,
                password: password
            )
            onSaved(record)
            dismiss()
        } catch {
            saveError = error.localizedDescription
        }
    }
}

