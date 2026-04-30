import SwiftUI

struct LegacyImportRootView: View {
    let profile: ServerProfileRecord

    @EnvironmentObject private var container: MacDependencyContainer
    @StateObject private var viewModel: LegacyMigrationViewModel
    @State private var pickerVisible = false
    @State private var passwordPromptVisible = false
    @State private var pendingAction: PendingAction?
    @State private var connectError: String?

    private enum PendingAction {
        case browse
        case scan
        case commit
    }

    init(
        profile: ServerProfileRecord,
        storageClientFactory: StorageClientFactory,
        profileStore: ProfileStore
    ) {
        self.profile = profile
        _viewModel = StateObject(wrappedValue: LegacyMigrationViewModel(
            profile: profile,
            storageClientFactory: storageClientFactory,
            profileStore: profileStore
        ))
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            ProfileHeader(profile: profile)
                .padding(.horizontal)
                .padding(.top)

            Divider()

            sourceSection
                .padding(.horizontal)

            Divider()

            content
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .sheet(isPresented: $pickerVisible) {
            if let client = viewModel.client {
                LegacyFolderPickerView(
                    client: client,
                    initialPath: viewModel.legacyFolderPath ?? defaultBrowseStart()
                ) { path in
                    viewModel.setLegacyPath(path)
                }
            }
        }
        .sheet(isPresented: $passwordPromptVisible) {
            StoragePasswordPromptView(
                profileName: profile.name,
                username: profile.username
            ) { password in
                Task { await connectAndExecute(action: pendingAction, password: password) }
            }
        }
        .alert(String(localized: "legacy.connect.failed"), isPresented: .constant(connectError != nil)) {
            Button(String(localized: "common.ok")) { connectError = nil }
        } message: {
            Text(connectError ?? "")
        }
    }

    @ViewBuilder
    private var sourceSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(String(localized: "migration.section.legacyFolder")).font(.headline)
            HStack {
                Text(viewModel.legacyFolderPath ?? String(localized: "common.notSelected"))
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .font(.body.monospaced())
                    .foregroundStyle(viewModel.legacyFolderPath == nil ? .secondary : .primary)
                Spacer()
                Button(String(localized: "legacy.folder.browse")) { handle(.browse) }
                    .disabled(viewModel.phase == .scanning || viewModel.phase == .committing)
            }
            .padding(8)
            .background(Color(nsColor: .controlBackgroundColor))
            .cornerRadius(6)

            if viewModel.phase == .scanned, let report = viewModel.report, !report.plans.isEmpty {
                Toggle(isOn: $viewModel.replaceSubsetAssets) {
                    VStack(alignment: .leading, spacing: 2) {
                        Text(String(localized: "migration.options.replaceSubsets.label"))
                        Text(String(localized: "migration.options.replaceSubsets.description"))
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
                .toggleStyle(.checkbox)
            }

            HStack {
                if viewModel.phase == .scanning {
                    ProgressView().controlSize(.small)
                    Text(String(localized: "migration.scanning")).foregroundStyle(.secondary)
                    Spacer()
                    Button(String(localized: "common.cancel")) { viewModel.cancelScan() }
                } else {
                    Spacer()
                    Button {
                        handle(.scan)
                    } label: {
                        Label(String(localized: "migration.button.scan"), systemImage: "magnifyingglass")
                    }
                    .keyboardShortcut("r", modifiers: [.command])
                    .disabled(viewModel.legacyFolderPath == nil ||
                              viewModel.phase == .committing ||
                              viewModel.phase == .committed)

                    if viewModel.phase == .scanned, let report = viewModel.report, !report.plans.isEmpty {
                        Button {
                            handle(.commit)
                        } label: {
                            Label(String(localized: "migration.button.commit"), systemImage: "tray.and.arrow.down")
                        }
                        .keyboardShortcut(.return, modifiers: [.command])
                        .buttonStyle(.borderedProminent)
                    }
                }
            }
        }
    }

    @ViewBuilder
    private var content: some View {
        switch viewModel.phase {
        case .idle:
            ContentUnavailableView(
                String(localized: "migration.idle.title"),
                systemImage: "folder.badge.questionmark",
                description: Text(String(localized: "migration.idle.detail"))
            )
        case .scanning:
            ProgressView(String(localized: "migration.scanning.text"))
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        case .scanned:
            if let report = viewModel.report {
                LegacyScanResultListView(report: report, storageType: profile.resolvedStorageType)
            } else {
                EmptyView()
            }
        case .committing, .committed, .error:
            LegacyMigrationProgressView(viewModel: viewModel)
        }
    }

    private func defaultBrowseStart() -> String {
        switch profile.resolvedStorageType {
        case .smb, .webdav, .externalVolume:
            return "/"
        }
    }

    // MARK: - Connect-then-execute

    private func handle(_ action: PendingAction) {
        connectError = nil
        if viewModel.client != nil {
            execute(action: action)
            return
        }
        pendingAction = action
        if profile.resolvedStorageType == .externalVolume {
            // No password needed; connect inline.
            Task { await connectAndExecute(action: action, password: "") }
            return
        }
        if let stored = try? container.profileStore.password(for: profile), !stored.isEmpty {
            Task { await connectAndExecute(action: action, password: stored) }
            return
        }
        passwordPromptVisible = true
    }

    private func connectAndExecute(action: PendingAction?, password: String) async {
        guard let action else { return }
        do {
            try await viewModel.connect(password: password)
            await MainActor.run {
                pendingAction = nil
                execute(action: action)
            }
        } catch {
            await MainActor.run {
                pendingAction = nil
                connectError = error.localizedDescription
            }
        }
    }

    private func execute(action: PendingAction) {
        switch action {
        case .browse: pickerVisible = true
        case .scan: viewModel.startScan()
        case .commit: viewModel.startCommit()
        }
    }
}

private struct ProfileHeader: View {
    let profile: ServerProfileRecord

    var body: some View {
        let display = StorageProfile(record: profile)
        HStack(spacing: 12) {
            Image(systemName: icon(for: display.storageType))
                .font(.title2)
                .foregroundStyle(.secondary)
            VStack(alignment: .leading, spacing: 2) {
                Text(profile.name).font(.title3.weight(.semibold))
                Text(display.displaySubtitle)
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .truncationMode(.middle)
            }
            Spacer()
        }
    }

    private func icon(for type: StorageType) -> String {
        switch type {
        case .smb: return "server.rack"
        case .webdav: return "network"
        case .externalVolume: return "externaldrive"
        }
    }
}
