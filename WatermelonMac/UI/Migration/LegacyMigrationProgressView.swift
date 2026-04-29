import SwiftUI

struct LegacyMigrationProgressView: View {
    @ObservedObject var viewModel: LegacyMigrationViewModel

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            header

            ProgressView(
                value: Double(viewModel.totals.bundlesProcessed),
                total: Double(max(viewModel.totals.bundlesPlanned, 1))
            ) {
                HStack {
                    if viewModel.phase == .committing {
                        if let month = viewModel.currentMonth {
                            Text(String(format: String(localized: "migration.progress.month.format"), month.text))
                                .font(.callout)
                        } else {
                            Text(String(localized: "migration.progress.connecting")).font(.callout)
                        }
                    } else if viewModel.phase == .committed {
                        Text(String(localized: "migration.progress.complete")).font(.callout)
                    }
                    Spacer()
                    Text("\(viewModel.totals.bundlesProcessed) / \(viewModel.totals.bundlesPlanned)")
                        .font(.callout.monospacedDigit())
                        .foregroundStyle(.secondary)
                }
            }
            .progressViewStyle(.linear)

            statsRow
            Divider()
            logScroll
        }
        .padding(.horizontal)
        .padding(.bottom, 12)
    }

    @ViewBuilder
    private var header: some View {
        HStack {
            switch viewModel.phase {
            case .committing:
                ProgressView().controlSize(.small)
                Text(String(localized: "migration.progress.importing")).font(.headline)
            case .committed:
                Image(systemName: "checkmark.seal.fill").foregroundStyle(.green)
                Text(String(localized: "migration.progress.imported")).font(.headline)
            case .error:
                Image(systemName: "xmark.octagon.fill").foregroundStyle(.red)
                Text(String(localized: "migration.progress.failed")).font(.headline)
            default:
                EmptyView()
            }
            Spacer()
            if viewModel.phase == .committing {
                Button(String(localized: "common.cancel")) { viewModel.cancelCommit() }
            } else {
                Button(String(localized: "common.done")) { viewModel.resetForNewScan() }
            }
        }
    }

    @ViewBuilder
    private var statsRow: some View {
        HStack(spacing: 18) {
            stat(String(localized: "migration.progress.stat.imported"), "\(viewModel.totals.bundlesImported)")
            stat(String(localized: "migration.progress.stat.skippedFp"), "\(viewModel.totals.bundlesSkippedFingerprintExists)")
            stat(String(localized: "migration.progress.stat.skippedHash"), "\(viewModel.totals.resourcesSkippedHashExists)")
            stat(String(localized: "migration.progress.stat.failed"), "\(viewModel.totals.bundlesFailed)")
            stat(String(localized: "migration.progress.stat.uploaded"), formatBytes(viewModel.totals.bytesUploaded))
            Spacer()
        }
    }

    @ViewBuilder
    private func stat(_ label: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 0) {
            Text(label).font(.caption).foregroundStyle(.secondary)
            Text(value).font(.body.monospacedDigit())
        }
    }

    @ViewBuilder
    private var logScroll: some View {
        ScrollViewReader { proxy in
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 2) {
                    ForEach(Array(viewModel.logLines.enumerated()), id: \.offset) { idx, line in
                        Text(line)
                            .font(.system(size: 11, design: .monospaced))
                            .foregroundStyle(.secondary)
                            .id(idx)
                    }
                }
                .padding(8)
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .background(Color(nsColor: .textBackgroundColor))
            .clipShape(RoundedRectangle(cornerRadius: 6))
            .onChange(of: viewModel.logLines.count) { _, newValue in
                if newValue > 0 {
                    withAnimation(nil) {
                        proxy.scrollTo(newValue - 1, anchor: .bottom)
                    }
                }
            }
        }
        .frame(minHeight: 160)
    }
}

private func formatBytes(_ bytes: Int64) -> String {
    ByteCountFormatter.fileSizeString(bytes)
}
