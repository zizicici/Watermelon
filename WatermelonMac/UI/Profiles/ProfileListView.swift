import SwiftUI

struct ProfileListView: View {
    @ObservedObject var store: ProfileStore
    let storageClientFactory: StorageClientFactory
    @Binding var selection: ServerProfileRecord.ID?
    @State private var addLocalSheetVisible = false
    @State private var addSMBSheetVisible = false
    @State private var addWebDAVSheetVisible = false
    @State private var addS3SheetVisible = false
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
                    Button {
                        addS3SheetVisible = true
                    } label: {
                        Label("S3…", systemImage: "cloud.fill")
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
            AddLocalProfileSheet(
                title: String(localized: "profile.add.local.title"),
                folderLabel: String(localized: "profile.add.local.backupRoot"),
                pickerMessage: String(localized: "profile.add.local.pickerMessage")
            ) { name, url in
                let record = try store.saveLocalProfile(name: name, folderURL: url)
                selection = record.id
            }
        }
        .sheet(isPresented: $addSMBSheetVisible) {
            AddSMBProfileSheet { context in
                let record = try store.saveSMBProfile(
                    name: context.auth.name,
                    host: context.auth.host,
                    port: context.auth.port,
                    shareName: context.shareName,
                    basePath: context.basePath,
                    username: context.auth.username,
                    domain: context.auth.domain,
                    password: context.auth.password
                )
                selection = record.id
            }
        }
        .sheet(isPresented: $addWebDAVSheetVisible) {
            AddWebDAVProfileSheet(
                storageClientFactory: storageClientFactory
            ) { snapshot in
                let record = try store.saveWebDAVProfile(
                    name: snapshot.name,
                    scheme: snapshot.scheme,
                    host: snapshot.host,
                    port: snapshot.port,
                    mountPath: snapshot.mountPath,
                    basePath: snapshot.basePath,
                    username: snapshot.username,
                    password: snapshot.password
                )
                selection = record.id
            }
        }
        .sheet(isPresented: $addS3SheetVisible) {
            AddS3ProfileSheet(
                storageClientFactory: storageClientFactory
            ) { snapshot in
                let record = try store.saveS3Profile(
                    name: snapshot.name,
                    scheme: snapshot.scheme,
                    host: snapshot.host,
                    port: snapshot.port,
                    region: snapshot.region,
                    bucket: snapshot.bucket,
                    basePath: snapshot.basePath,
                    usePathStyle: snapshot.usePathStyle,
                    accessKeyID: snapshot.accessKeyID,
                    secretAccessKey: snapshot.secretAccessKey
                )
                selection = record.id
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
        case .s3: return "cloud.fill"
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
