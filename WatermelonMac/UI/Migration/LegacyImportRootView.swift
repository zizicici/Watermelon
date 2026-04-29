import SwiftUI

struct LegacyImportRootView: View {
    let profile: ServerProfileRecord

    @EnvironmentObject private var container: MacDependencyContainer
    @StateObject private var viewModel: LegacyMigrationViewModel
    @State private var passwordPromptVisible = false

    init(
        profile: ServerProfileRecord,
        storageClientFactory: StorageClientFactory,
        profileStore: ProfileStore
    ) {
        self.profile = profile
        _viewModel = StateObject(wrappedValue: LegacyMigrationViewModel(
            storageClientFactory: storageClientFactory,
            profileStore: profileStore,
            profileID: profile.id
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
        .sheet(isPresented: $passwordPromptVisible) {
            StoragePasswordPromptView(
                profileName: profile.name,
                username: profile.username
            ) { password in
                viewModel.startCommit(profile: profile, password: password)
            }
        }
    }

    @ViewBuilder
    private var sourceSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(String(localized: "migration.section.legacyFolder")).font(.headline)
            HStack {
                Text(viewModel.sourceFolderURL?.path ?? String(localized: "common.notSelected"))
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .foregroundStyle(viewModel.sourceFolderURL == nil ? .secondary : .primary)
                Spacer()
                Button(String(localized: "common.choose.folder")) { viewModel.pickSourceFolder() }
                    .disabled(viewModel.phase == .scanning || viewModel.phase == .committing)
            }
            .padding(8)
            .background(Color(nsColor: .controlBackgroundColor))
            .cornerRadius(6)

            HStack {
                if viewModel.phase == .scanning {
                    ProgressView().controlSize(.small)
                    Text(String(localized: "migration.scanning")).foregroundStyle(.secondary)
                    Spacer()
                    Button(String(localized: "common.cancel")) { viewModel.cancelScan() }
                } else {
                    Spacer()
                    Button {
                        viewModel.startScan()
                    } label: {
                        Label(String(localized: "migration.button.scan"), systemImage: "magnifyingglass")
                    }
                    .keyboardShortcut("r", modifiers: [.command])
                    .disabled(viewModel.sourceFolderURL == nil ||
                              viewModel.phase == .committing ||
                              viewModel.phase == .committed)

                    if viewModel.phase == .scanned, let report = viewModel.report, !report.plans.isEmpty {
                        Button {
                            launchCommit()
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

    private func launchCommit() {
        let display = StorageProfile(record: profile)
        if !display.requiresPassword {
            viewModel.startCommit(profile: profile, password: "")
            return
        }
        if let stored = try? container.profileStore.password(for: profile), !stored.isEmpty {
            viewModel.startCommit(profile: profile, password: stored)
            return
        }
        passwordPromptVisible = true
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
