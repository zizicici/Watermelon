import SwiftUI

struct ProfileListView: View {
    @ObservedObject var store: ProfileStore
    let storageClientFactory: StorageClientFactory
    @Binding var selection: ServerProfileRecord.ID?
    @State private var addLocalSheetVisible = false
    @State private var addSMBSheetVisible = false
    @State private var addWebDAVSheetVisible = false
    @State private var renamingProfile: ServerProfileRecord?

    var body: some View {
        VStack(spacing: 0) {
            List(selection: $selection) {
                ForEach(store.profiles) { profile in
                    ProfileRow(profile: profile)
                        .tag(profile.id)
                        .contextMenu {
                            Button {
                                renamingProfile = profile
                            } label: {
                                Label(String(localized: "profile.rename.menu"), systemImage: "pencil")
                            }
                            Divider()
                            Button(role: .destructive) {
                                if let id = profile.id {
                                    try? store.deleteProfile(id: id)
                                    if selection == id { selection = nil }
                                }
                            } label: {
                                Label(String(localized: "common.delete"), systemImage: "trash")
                            }
                        }
                }
            }
            .listStyle(.sidebar)

            Divider()

            HStack(spacing: 8) {
                Menu {
                    Button {
                        addLocalSheetVisible = true
                    } label: {
                        Label(String(localized: "profiles.add.local"), systemImage: "folder")
                    }
                    Button {
                        addSMBSheetVisible = true
                    } label: {
                        Label("SMB…", systemImage: "server.rack")
                    }
                    Button {
                        addWebDAVSheetVisible = true
                    } label: {
                        Label("WebDAV…", systemImage: "network")
                    }
                } label: {
                    Label(String(localized: "profiles.add"), systemImage: "plus")
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .menuStyle(.borderlessButton)
                .padding(.vertical, 6)
                .padding(.horizontal, 10)
                Spacer()
            }
        }
        .navigationTitle("Watermelon")
        .sheet(isPresented: $addLocalSheetVisible) {
            AddLocalProfileSheet(store: store) { newProfile in
                selection = newProfile.id
            }
        }
        .sheet(isPresented: $addSMBSheetVisible) {
            AddSMBProfileSheet(store: store) { newProfile in
                selection = newProfile.id
            }
        }
        .sheet(isPresented: $addWebDAVSheetVisible) {
            AddWebDAVProfileSheet(
                store: store,
                storageClientFactory: storageClientFactory
            ) { newProfile in
                selection = newProfile.id
            }
        }
        .sheet(item: $renamingProfile) { profile in
            if let id = profile.id {
                RenameProfileSheet(store: store, profileID: id, initialName: profile.name)
            }
        }
        .alert("Failed to load profiles", isPresented: .constant(store.loadError != nil)) {
            Button("OK") { store.loadError = nil }
        } message: {
            Text(store.loadError?.localizedDescription ?? "")
        }
    }
}

private struct ProfileRow: View {
    let profile: ServerProfileRecord

    private var icon: String {
        switch profile.resolvedStorageType {
        case .smb: return "server.rack"
        case .webdav: return "network"
        case .externalVolume: return "externaldrive"
        }
    }

    var body: some View {
        HStack(spacing: 10) {
            Image(systemName: icon)
                .frame(width: 22)
                .foregroundStyle(.secondary)
            VStack(alignment: .leading, spacing: 2) {
                Text(profile.name).font(.body)
                Text(StorageProfile(record: profile).displaySubtitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .truncationMode(.middle)
            }
            Spacer()
        }
        .padding(.vertical, 2)
    }
}
