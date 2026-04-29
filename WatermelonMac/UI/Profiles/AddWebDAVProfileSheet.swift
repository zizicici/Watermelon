import SwiftUI

struct WebDAVProfileSnapshot {
    let name: String
    let scheme: String
    let host: String
    let port: Int
    let mountPath: String
    let basePath: String
    let username: String
    let password: String
}

struct AddWebDAVProfileSheet: View {
    let storageClientFactory: StorageClientFactory
    let title: String
    let basePathLabel: String
    let basePathDefault: String
    let save: (_ snapshot: WebDAVProfileSnapshot) throws -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var name: String = ""
    @State private var scheme: String = "https"
    @State private var host: String = ""
    @State private var portString: String = ""
    @State private var mountPath: String = "/"
    @State private var basePath: String
    @State private var username: String = ""
    @State private var password: String = ""
    @State private var saveError: String?
    @State private var verifyMessage: String?
    @State private var verifying = false
    @State private var saving = false

    init(
        storageClientFactory: StorageClientFactory,
        title: String = String(localized: "profile.add.webdav.title"),
        basePathLabel: String = String(localized: "profile.add.webdav.basePath"),
        basePathDefault: String = "/Watermelon",
        save: @escaping (_ snapshot: WebDAVProfileSnapshot) throws -> Void
    ) {
        self.storageClientFactory = storageClientFactory
        self.title = title
        self.basePathLabel = basePathLabel
        self.basePathDefault = basePathDefault
        self.save = save
        _basePath = State(initialValue: basePathDefault)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text(title).font(.headline)

            Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 10) {
                row(String(localized: "smb.field.name"), placeholder: String(localized: "smb.field.name.placeholder"), text: $name)
                GridRow {
                    Text(String(localized: "profile.add.webdav.scheme")).gridColumnAlignment(.trailing).foregroundStyle(.secondary)
                    Picker("", selection: $scheme) {
                        Text("https").tag("https")
                        Text("http").tag("http")
                    }
                    .labelsHidden()
                    .pickerStyle(.segmented)
                    .frame(width: 160, alignment: .leading)
                }
                row(String(localized: "smb.field.host"), placeholder: "e.g. example.com", text: $host)
                row(String(localized: "smb.field.port"), placeholder: String(localized: "profile.add.webdav.port.placeholder"), text: $portString)
                row(String(localized: "profile.add.webdav.mountPath"), placeholder: "/", text: $mountPath)
                row(basePathLabel, placeholder: basePathDefault, text: $basePath)
                row(String(localized: "smb.field.username"), placeholder: "", text: $username)
                GridRow {
                    Text(String(localized: "smb.field.password")).gridColumnAlignment(.trailing).foregroundStyle(.secondary)
                    SecureField("", text: $password).textFieldStyle(.roundedBorder)
                }
            }

            if let verifyMessage {
                Text(verifyMessage).font(.callout).foregroundStyle(.secondary)
            }
            if let saveError {
                Text(saveError).font(.callout).foregroundStyle(.red)
            }

            HStack {
                Button(verifying ? String(localized: "profile.add.webdav.verifying") : String(localized: "profile.add.webdav.verify")) { verify() }
                    .disabled(!hasMinimumFields || verifying || saving)
                Spacer()
                Button(String(localized: "common.cancel")) { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button(saving ? String(localized: "common.saving") : String(localized: "common.save")) { commit() }
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
        guard let snapshot = makeProbeRecord() else {
            saveError = String(localized: "profile.add.webdav.error.invalidEndpoint")
            return
        }
        verifying = true
        Task { [storageClientFactory] in
            do {
                let client = try storageClientFactory.makeClient(profile: snapshot, password: password)
                try await client.connect()
                _ = try await client.list(path: RemotePathBuilder.normalizePath(snapshot.shareName))
                await MainActor.run {
                    verifyMessage = String(localized: "profile.add.webdav.verify.success")
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

    private func commit() {
        saving = true
        defer { saving = false }
        let snapshot = WebDAVProfileSnapshot(
            name: name.trimmed,
            scheme: scheme,
            host: host.trimmed,
            port: resolvedPort(),
            mountPath: mountPath.trimmed.isEmpty ? "/" : mountPath.trimmed,
            basePath: basePath.trimmed.isEmpty ? "/" : basePath.trimmed,
            username: username.trimmed,
            password: password
        )
        do {
            try save(snapshot)
            dismiss()
        } catch {
            saveError = error.localizedDescription
        }
    }

    private func makeProbeRecord() -> ServerProfileRecord? {
        let port = resolvedPort()
        let normalizedScheme = scheme.lowercased()
        let normalizedMount = RemotePathBuilder.normalizePath(mountPath.trimmed.isEmpty ? "/" : mountPath.trimmed)
        guard ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: normalizedScheme,
            host: host.trimmed,
            port: port,
            mountPath: normalizedMount
        ) != nil else { return nil }

        let params = WebDAVConnectionParams(scheme: normalizedScheme)
        guard let encoded = try? ServerProfileRecord.encodedConnectionParams(params) else { return nil }

        return ServerProfileRecord(
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
    }
}
