import AppKit
import SwiftUI

struct AddLocalProfileSheet: View {
    @ObservedObject var store: ProfileStore
    let onSaved: (ServerProfileRecord) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var name: String = ""
    @State private var folderURL: URL?
    @State private var saveError: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Add Local Folder Profile")
                .font(.headline)

            VStack(alignment: .leading, spacing: 6) {
                Text("Profile Name").font(.callout).foregroundStyle(.secondary)
                TextField("Optional — defaults to folder name", text: $name)
                    .textFieldStyle(.roundedBorder)
            }

            VStack(alignment: .leading, spacing: 6) {
                Text("Backup Root Folder").font(.callout).foregroundStyle(.secondary)
                HStack {
                    Text(folderURL?.path ?? "Not selected")
                        .lineLimit(1)
                        .truncationMode(.middle)
                        .foregroundStyle(folderURL == nil ? .secondary : .primary)
                    Spacer()
                    Button("Choose…") { pickFolder() }
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
                Button("Cancel") { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button("Save") { save() }
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
        panel.message = "Select the Watermelon backup root folder"
        panel.prompt = "Choose"
        if panel.runModal() == .OK, let url = panel.url {
            folderURL = url
            if name.isEmpty {
                name = url.lastPathComponent
            }
        }
    }

    private func save() {
        guard let folderURL else { return }
        do {
            let record = try store.saveLocalProfile(name: name, folderURL: folderURL)
            onSaved(record)
            dismiss()
        } catch {
            saveError = error.localizedDescription
        }
    }
}
