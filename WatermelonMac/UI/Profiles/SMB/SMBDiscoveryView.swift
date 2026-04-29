import SwiftUI

struct SMBDiscoveryView: View {
    @StateObject private var viewModel = SMBDiscoveryViewModel()
    let onPick: (_ name: String, _ host: String, _ port: Int) -> Void
    let onManualEntry: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text(String(localized: "smb.discovery.title")).font(.headline)
                Spacer()
                if viewModel.isBrowsing {
                    ProgressView().controlSize(.small)
                } else {
                    Button {
                        viewModel.startDiscovery()
                    } label: {
                        Label(String(localized: "smb.discovery.refresh"), systemImage: "arrow.clockwise")
                    }
                    .labelStyle(.iconOnly)
                }
            }

            if let error = viewModel.browserError {
                Text(error).font(.callout).foregroundStyle(.orange)
            }

            List(viewModel.rows) { row in
                Button {
                    if let host = row.host, let port = row.port {
                        onPick(row.name, host, port)
                    }
                } label: {
                    HStack {
                        Image(systemName: "server.rack")
                            .foregroundStyle(.secondary)
                        VStack(alignment: .leading, spacing: 2) {
                            Text(row.name).font(.body)
                            if let host = row.host, let port = row.port {
                                Text("\(host):\(port)").font(.caption).foregroundStyle(.secondary)
                            } else if let err = row.error {
                                Text(err).font(.caption).foregroundStyle(.orange)
                            } else {
                                Text(String(localized: "smb.discovery.resolving")).font(.caption).foregroundStyle(.secondary)
                            }
                        }
                        Spacer()
                        if row.isReady {
                            Image(systemName: "chevron.right").foregroundStyle(.secondary)
                        }
                    }
                }
                .buttonStyle(.plain)
                .disabled(!row.isReady)
            }
            .listStyle(.bordered)
            .frame(minHeight: 200)

            if !viewModel.isBrowsing && viewModel.rows.isEmpty && viewModel.browserError == nil {
                Text(String(localized: "smb.discovery.empty"))
                    .font(.callout)
                    .foregroundStyle(.secondary)
            }

            HStack {
                Spacer()
                Button(String(localized: "smb.discovery.manualEntry")) { onManualEntry() }
            }
        }
        .onAppear { viewModel.startDiscovery() }
    }
}
