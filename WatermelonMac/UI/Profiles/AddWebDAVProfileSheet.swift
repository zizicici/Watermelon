import SwiftUI

struct AddWebDAVProfileSheet: View {
    @ObservedObject var store: ProfileStore
    let storageClientFactory: StorageClientFactory
    let onSaved: (ServerProfileRecord) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var name: String = ""
    @State private var scheme: String = "https"
    @State private var host: String = ""
    @State private var portString: String = ""
    @State private var mountPath: String = "/"
    @State private var basePath: String = "/Watermelon"
    @State private var username: String = ""
    @State private var password: String = ""
    @State private var saveError: String?
    @State private var verifyMessage: String?
    @State private var verifying = false
    @State private var saving = false

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Add WebDAV Profile").font(.headline)

            Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 10) {
                row("Name", placeholder: "Optional — defaults to host", text: $name)
                GridRow {
                    Text("Scheme").gridColumnAlignment(.trailing).foregroundStyle(.secondary)
                    Picker("", selection: $scheme) {
                        Text("https").tag("https")
                        Text("http").tag("http")
                    }
                    .labelsHidden()
                    .pickerStyle(.segmented)
                    .frame(width: 160, alignment: .leading)
                }
                row("Host", placeholder: "e.g. example.com", text: $host)
                row("Port", placeholder: "Optional (443 / 80)", text: $portString)
                row("Mount path", placeholder: "/", text: $mountPath)
                row("Base path", placeholder: "/Watermelon", text: $basePath)
                row("Username", placeholder: "", text: $username)
                GridRow {
                    Text("Password").gridColumnAlignment(.trailing).foregroundStyle(.secondary)
                    SecureField("", text: $password)
                        .textFieldStyle(.roundedBorder)
                }
            }

            if let verifyMessage {
                Text(verifyMessage).font(.callout).foregroundStyle(.secondary)
            }
            if let saveError {
                Text(saveError).font(.callout).foregroundStyle(.red)
            }

            HStack {
                Button(verifying ? "Verifying…" : "Verify") { verify() }
                    .disabled(!hasMinimumFields || verifying || saving)
                Spacer()
                Button("Cancel") { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button(saving ? "Saving…" : "Save") { save() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(!hasMinimumFields || saving)
            }
        }
        .padding(20)
        .frame(width: 520)
    }

    @ViewBuilder
    private func row(_ label: String, placeholder: String, text: Binding<String>) -> some View {
        GridRow {
            Text(label).gridColumnAlignment(.trailing).foregroundStyle(.secondary)
            TextField(placeholder, text: text)
                .textFieldStyle(.roundedBorder)
        }
    }

    private var hasMinimumFields: Bool {
        !host.trimmed.isEmpty && !username.trimmed.isEmpty && !password.isEmpty
    }

    private func resolvedPort() -> Int {
        if let value = Int(portString.trimmed), value > 0 { return value }
        return scheme == "https" ? 443 : 80
    }

    private func verify() {
        verifyMessage = nil
        saveError = nil
        let snapshot = makeSnapshotRecord()
        guard let snapshot else {
            saveError = "Invalid endpoint"
            return
        }
        verifying = true
        Task { [storageClientFactory] in
            do {
                let client = try storageClientFactory.makeClient(profile: snapshot.record, password: password)
                try await client.connect()
                _ = try await client.list(path: RemotePathBuilder.normalizePath(snapshot.record.shareName))
                await MainActor.run {
                    verifyMessage = "Connection successful"
                    verifying = false
                }
                await client.disconnect()
            } catch {
                await MainActor.run {
                    saveError = error.localizedDescription
                    verifying = false
                }
            }
        }
    }

    private func save() {
        saving = true
        defer { saving = false }
        do {
            let record = try store.saveWebDAVProfile(
                name: name,
                scheme: scheme,
                host: host.trimmed,
                port: resolvedPort(),
                mountPath: mountPath.trimmed.isEmpty ? "/" : mountPath.trimmed,
                basePath: basePath.trimmed.isEmpty ? "/" : basePath.trimmed,
                username: username.trimmed,
                password: password
            )
            onSaved(record)
            dismiss()
        } catch {
            saveError = error.localizedDescription
        }
    }

    private struct ProfileSnapshot {
        let record: ServerProfileRecord
    }

    private func makeSnapshotRecord() -> ProfileSnapshot? {
        let port = resolvedPort()
        let normalizedScheme = scheme.lowercased()
        let normalizedMount = RemotePathBuilder.normalizePath(mountPath.trimmed.isEmpty ? "/" : mountPath.trimmed)
        guard let _ = ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: normalizedScheme,
            host: host.trimmed,
            port: port,
            mountPath: normalizedMount
        ) else { return nil }

        let params = WebDAVConnectionParams(scheme: normalizedScheme)
        guard let encoded = try? ServerProfileRecord.encodedConnectionParams(params) else { return nil }

        let record = ServerProfileRecord(
            id: nil,
            name: name.trimmed.isEmpty ? host.trimmed : name.trimmed,
            storageType: StorageType.webdav.rawValue,
            connectionParams: encoded,
            sortOrder: 0,
            host: host.trimmed,
            port: port,
            shareName: normalizedMount,
            basePath: RemotePathBuilder.normalizePath(basePath.trimmed.isEmpty ? "/" : basePath.trimmed),
            username: username.trimmed,
            domain: nil,
            credentialRef: "",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
        return ProfileSnapshot(record: record)
    }
}

