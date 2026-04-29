import AppKit
import SwiftUI

struct AddLocalProfileSheet: View {
    let title: String
    let folderLabel: String
    let pickerMessage: String
    let save: (_ name: String, _ folderURL: URL) throws -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var name: String = ""
    @State private var folderURL: URL?
    @State private var saveError: String?

    init(
        title: String = String(localized: "profile.add.local.title"),
        folderLabel: String = String(localized: "profile.add.local.folder"),
        pickerMessage: String = String(localized: "profile.add.local.pickerMessage"),
        save: @escaping (_ name: String, _ folderURL: URL) throws -> Void
    ) {
        self.title = title
        self.folderLabel = folderLabel
        self.pickerMessage = pickerMessage
        self.save = save
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text(title).font(.headline)

            VStack(alignment: .leading, spacing: 6) {
                Text(String(localized: "profile.add.local.name")).font(.callout).foregroundStyle(.secondary)
                TextField(String(localized: "profile.add.local.name.placeholder"), text: $name)
                    .textFieldStyle(.roundedBorder)
            }

            VStack(alignment: .leading, spacing: 6) {
                Text(folderLabel).font(.callout).foregroundStyle(.secondary)
                HStack {
                    Text(folderURL?.path ?? String(localized: "common.notSelected"))
                        .lineLimit(1)
                        .truncationMode(.middle)
                        .foregroundStyle(folderURL == nil ? .secondary : .primary)
                    Spacer()
                    Button(String(localized: "common.choose")) { pickFolder() }
                }
                .padding(8)
                .background(Color(nsColor: .controlBackgroundColor))
                .cornerRadius(6)
            }

            if let saveError {
                Text(saveError)
                    .font(.callout)
                    .foregroundStyle(.red)
            }

            HStack {
                Spacer()
                Button(String(localized: "common.cancel")) { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button(String(localized: "common.save")) { commit() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(folderURL == nil)
            }
        }
        .padding(20)
        .frame(width: 480)
    }

    private func pickFolder() {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.canCreateDirectories = true
        panel.message = pickerMessage
        panel.prompt = String(localized: "common.choose")
        if panel.runModal() == .OK, let url = panel.url {
            folderURL = url
            if name.isEmpty {
                name = url.lastPathComponent
            }
        }
    }

    private func commit() {
        guard let folderURL else { return }
        do {
            try save(name, folderURL)
            dismiss()
        } catch {
            saveError = error.localizedDescription
        }
    }
}
