import SwiftUI

struct SMBSharePickerView: View {
    @ObservedObject var viewModel: SMBSharePickerViewModel

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            sharesSection
            if viewModel.selectedShareName != nil {
                Divider()
                directorySection
            }
        }
        .onAppear {
            if viewModel.sharesState == .idle {
                viewModel.loadShares()
            }
        }
    }

    @ViewBuilder
    private var sharesSection: some View {
        Text(String(localized: "smb.share.title")).font(.headline)
        switch viewModel.sharesState {
        case .idle, .loading:
            HStack {
                ProgressView().controlSize(.small)
                Text(String(localized: "smb.share.loading")).foregroundStyle(.secondary)
            }
        case .failed(let message):
            VStack(alignment: .leading, spacing: 4) {
                Text(message).font(.callout).foregroundStyle(.red)
                Button(String(localized: "common.retry")) { viewModel.loadShares() }
            }
        case .loaded:
            if viewModel.shares.isEmpty {
                Text(String(localized: "smb.share.empty")).foregroundStyle(.secondary)
            } else {
                List(viewModel.shares, id: \.name) { share in
                    Button {
                        viewModel.selectShare(share.name)
                    } label: {
                        HStack {
                            Image(systemName: "externaldrive.connected.to.line.below")
                                .foregroundStyle(.secondary)
                            VStack(alignment: .leading, spacing: 2) {
                                Text(share.name)
                                if !share.comment.isEmpty {
                                    Text(share.comment).font(.caption).foregroundStyle(.secondary)
                                }
                            }
                            Spacer()
                            if viewModel.selectedShareName == share.name {
                                Image(systemName: "checkmark").foregroundStyle(.tint)
                            }
                        }
                    }
                    .buttonStyle(.plain)
                }
                .listStyle(.bordered)
                .frame(minHeight: 120)
            }
        }
    }

    @ViewBuilder
    private var directorySection: some View {
        HStack {
            Text(String(localized: "smb.path.title")).font(.headline)
            Spacer()
            Text(viewModel.currentPath)
                .font(.caption.monospaced())
                .foregroundStyle(.secondary)
                .lineLimit(1)
                .truncationMode(.middle)
        }

        switch viewModel.directoryState {
        case .idle:
            EmptyView()
        case .loading:
            HStack {
                ProgressView().controlSize(.small)
                Text(String(localized: "smb.path.loading")).foregroundStyle(.secondary)
            }
        case .failed(let message):
            VStack(alignment: .leading, spacing: 4) {
                Text(message).font(.callout).foregroundStyle(.red)
                Button(String(localized: "common.retry")) { viewModel.retryDirectory() }
            }
        case .loaded:
            List {
                if viewModel.currentPath != "/" {
                    Button {
                        viewModel.navigateUp()
                    } label: {
                        HStack {
                            Image(systemName: "arrow.up.doc")
                            Text("..")
                            Spacer()
                        }
                    }
                    .buttonStyle(.plain)
                }
                if viewModel.directoryEntries.isEmpty {
                    Text(String(localized: "smb.path.emptyDir")).foregroundStyle(.secondary)
                } else {
                    ForEach(viewModel.directoryEntries, id: \.path) { entry in
                        Button {
                            viewModel.navigate(to: entry.path)
                        } label: {
                            HStack {
                                Image(systemName: "folder")
                                    .foregroundStyle(.secondary)
                                Text(entry.name)
                                Spacer()
                                Image(systemName: "chevron.right").foregroundStyle(.secondary)
                            }
                        }
                        .buttonStyle(.plain)
                    }
                }
            }
            .listStyle(.bordered)
            .frame(minHeight: 200)
        }
    }
}
