import SwiftUI

struct RenameProfileSheet: View {
    @ObservedObject var store: ProfileStore
    let profileID: Int64
    let initialName: String

    @Environment(\.dismiss) private var dismiss
    @State private var name: String
    @State private var saveError: String?

    init(store: ProfileStore, profileID: Int64, initialName: String) {
        self.store = store
        self.profileID = profileID
        self.initialName = initialName
        _name = State(initialValue: initialName)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text(String(localized: "profile.rename.title")).font(.headline)
            TextField("", text: $name)
                .textFieldStyle(.roundedBorder)
                .onSubmit { save() }
            if let saveError {
                Text(saveError).font(.callout).foregroundStyle(.red)
            }
            HStack {
                Spacer()
                Button(String(localized: "common.cancel")) { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button(String(localized: "common.save")) { save() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
        }
        .padding(20)
        .frame(width: 360)
    }

    private func save() {
        do {
            try store.renameProfile(id: profileID, newName: name)
            dismiss()
        } catch {
            saveError = error.localizedDescription
        }
    }
}
