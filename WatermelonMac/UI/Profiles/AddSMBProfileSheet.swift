import SwiftUI

struct AddSMBProfileSheet: View {
    let save: (_ context: SMBServerPathContext) throws -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var step: Step = .discovery

    @State private var name: String = ""
    @State private var host: String = ""
    @State private var portString: String = "445"
    @State private var username: String = ""
    @State private var domain: String = ""
    @State private var password: String = ""

    @State private var sharePickerViewModel: SMBSharePickerViewModel?
    @State private var saveError: String?
    @State private var saving = false

    private enum Step: Equatable {
        case discovery
        case credentials
        case sharePath
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                if step != .discovery {
                    Button {
                        goBack()
                    } label: {
                        Label(String(localized: "common.back"), systemImage: "chevron.left")
                    }
                    .labelStyle(.iconOnly)
                }
                Text(stepTitle).font(.headline)
                Spacer()
            }

            content

            if let saveError {
                Text(saveError).font(.callout).foregroundStyle(.red)
            }

            HStack {
                Spacer()
                Button(String(localized: "common.cancel")) { dismiss() }
                    .keyboardShortcut(.cancelAction)
                trailingActionButton
            }
        }
        .padding(20)
        .frame(width: 560, height: 540)
    }

    private var stepTitle: String {
        switch step {
        case .discovery: return String(localized: "smb.add.title.discovery")
        case .credentials: return String(localized: "smb.add.title.credentials")
        case .sharePath: return String(localized: "smb.add.title.sharePath")
        }
    }

    @ViewBuilder
    private var content: some View {
        switch step {
        case .discovery:
            SMBDiscoveryView(
                onPick: { rowName, rowHost, rowPort in
                    if name.isEmpty { name = rowName }
                    host = rowHost
                    portString = String(rowPort)
                    step = .credentials
                },
                onManualEntry: {
                    step = .credentials
                }
            )
        case .credentials:
            credentialsForm
        case .sharePath:
            if let vm = sharePickerViewModel {
                SMBSharePickerView(viewModel: vm)
            }
        }
    }

    @ViewBuilder
    private var credentialsForm: some View {
        Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 10) {
            row(String(localized: "smb.field.name"), placeholder: String(localized: "smb.field.name.placeholder"), text: $name)
            row(String(localized: "smb.field.host"), placeholder: "e.g. 192.168.1.10", text: $host)
            row(String(localized: "smb.field.port"), placeholder: "445", text: $portString)
            row(String(localized: "smb.field.username"), placeholder: "", text: $username)
            row(String(localized: "smb.field.domain"), placeholder: String(localized: "smb.field.domain.placeholder"), text: $domain)
            GridRow {
                Text(String(localized: "smb.field.password")).gridColumnAlignment(.trailing).foregroundStyle(.secondary)
                SecureField("", text: $password).textFieldStyle(.roundedBorder)
            }
        }
        Text(String(localized: "smb.field.domain.footer"))
            .font(.caption)
            .foregroundStyle(.secondary)
    }

    @ViewBuilder
    private func row(_ label: String, placeholder: String, text: Binding<String>) -> some View {
        GridRow {
            Text(label).gridColumnAlignment(.trailing).foregroundStyle(.secondary)
            TextField(placeholder, text: text).textFieldStyle(.roundedBorder)
        }
    }

    @ViewBuilder
    private var trailingActionButton: some View {
        switch step {
        case .discovery:
            Button(String(localized: "smb.add.action.next")) { step = .credentials }
                .keyboardShortcut(.defaultAction)
        case .credentials:
            Button(saving ? String(localized: "common.saving") : String(localized: "smb.add.action.connect")) {
                advanceToSharePath()
            }
            .keyboardShortcut(.defaultAction)
            .disabled(!credentialsValid || saving)
        case .sharePath:
            Button(saving ? String(localized: "common.saving") : String(localized: "common.save")) {
                commit()
            }
            .keyboardShortcut(.defaultAction)
            .disabled(saving || sharePickerViewModel?.canCommit != true)
        }
    }

    private var credentialsValid: Bool {
        !host.trimmed.isEmpty &&
        !username.trimmed.isEmpty &&
        !password.isEmpty &&
        Int(portString) != nil
    }

    private func goBack() {
        saveError = nil
        switch step {
        case .discovery: break
        case .credentials: step = .discovery
        case .sharePath:
            sharePickerViewModel = nil
            step = .credentials
        }
    }

    private func advanceToSharePath() {
        saveError = nil
        guard let port = Int(portString) else {
            saveError = String(localized: "smb.error.invalidPort")
            return
        }
        let auth = SMBServerAuthContext(
            name: name.trimmed.isEmpty ? host.trimmed : name.trimmed,
            host: host.trimmed,
            port: port,
            username: username.trimmed,
            password: password,
            domain: domain.trimmed.isEmpty ? nil : domain.trimmed
        )
        sharePickerViewModel = SMBSharePickerViewModel(auth: auth)
        step = .sharePath
    }

    private func commit() {
        guard let context = sharePickerViewModel?.commitContext else { return }
        saving = true
        defer { saving = false }
        do {
            try save(context)
            dismiss()
        } catch {
            saveError = error.localizedDescription
        }
    }
}
