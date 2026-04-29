import SwiftUI

struct LegacyFolderPickerView: View {
    @StateObject private var viewModel: LegacyFolderPickerViewModel
    let onPick: (_ path: String) -> Void

    @Environment(\.dismiss) private var dismiss

    init(
        client: any RemoteStorageClientProtocol,
        initialPath: String,
        onPick: @escaping (_ path: String) -> Void
    ) {
        _viewModel = StateObject(wrappedValue: LegacyFolderPickerViewModel(client: client, initialPath: initialPath))
        self.onPick = onPick
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text(String(localized: "legacy.folder.picker.title")).font(.headline)
                Spacer()
            }
            HStack(spacing: 8) {
                Button {
                    viewModel.navigateUp()
                } label: {
                    Image(systemName: "arrow.up")
                }
                .disabled(!viewModel.canGoUp)

                Text(viewModel.currentPath)
                    .font(.body.monospaced())
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .frame(maxWidth: .infinity, alignment: .leading)

                Button {
                    viewModel.load()
                } label: {
                    Image(systemName: "arrow.clockwise")
                }
            }
            .padding(8)
            .background(Color(nsColor: .controlBackgroundColor))
            .cornerRadius(6)

            content

            HStack {
                Text(String(localized: "legacy.folder.picker.hint"))
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
                Button(String(localized: "common.cancel")) { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button(String(localized: "legacy.folder.picker.useThis")) {
                    onPick(viewModel.currentPath)
                    dismiss()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(viewModel.state != .loaded)
            }
        }
        .padding(20)
        .frame(width: 560, height: 480)
        .onAppear { viewModel.load() }
    }

    @ViewBuilder
    private var content: some View {
        switch viewModel.state {
        case .idle, .loading:
            HStack {
                ProgressView().controlSize(.small)
                Text(String(localized: "smb.path.loading")).foregroundStyle(.secondary)
                Spacer()
            }
            .frame(maxHeight: .infinity)
        case .failed(let message):
            VStack(alignment: .leading, spacing: 8) {
                Text(message).font(.callout).foregroundStyle(.red)
                Button(String(localized: "common.retry")) { viewModel.load() }
            }
            .frame(maxHeight: .infinity, alignment: .topLeading)
        case .loaded:
            if viewModel.entries.isEmpty {
                Text(String(localized: "smb.path.emptyDir"))
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                List(viewModel.entries, id: \.path) { entry in
                    Button {
                        viewModel.navigate(to: entry.path)
                    } label: {
                        HStack {
                            Image(systemName: "folder").foregroundStyle(.secondary)
                            Text(entry.name)
                            Spacer()
                            Image(systemName: "chevron.right").foregroundStyle(.secondary)
                        }
                    }
                    .buttonStyle(.plain)
                }
                .listStyle(.bordered)
            }
        }
    }
}
