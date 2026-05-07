import SwiftUI

struct S3ProfileSnapshot {
    let name: String
    let scheme: String
    let host: String
    let port: Int
    let region: String
    let bucket: String
    let basePath: String
    let usePathStyle: Bool
    let accessKeyID: String
    let secretAccessKey: String
}

struct AddS3ProfileSheet: View {
    let storageClientFactory: StorageClientFactory
    let title: String
    let basePathDefault: String
    let save: (_ snapshot: S3ProfileSnapshot) throws -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var name: String = ""
    @State private var endpoint: String = ""
    @State private var region: String = ""
    @State private var bucket: String = ""
    @State private var basePath: String
    @State private var accessKey: String = ""
    @State private var secretKey: String = ""
    @State private var pathStyleOverride: Bool?
    @State private var saveError: String?
    @State private var verifyMessage: String?
    @State private var verifying = false
    @State private var saving = false

    init(
        storageClientFactory: StorageClientFactory,
        title: String = String(localized: "profile.add.s3.title"),
        basePathDefault: String = "/Watermelon",
        save: @escaping (_ snapshot: S3ProfileSnapshot) throws -> Void
    ) {
        self.storageClientFactory = storageClientFactory
        self.title = title
        self.basePathDefault = basePathDefault
        self.save = save
        _basePath = State(initialValue: basePathDefault)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text(title).font(.headline)

            Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 10) {
                row(String(localized: "smb.field.name"), placeholder: String(localized: "smb.field.name.placeholder"), text: $name)
                row(String(localized: "profile.add.s3.endpoint"), placeholder: "https://s3.us-east-1.amazonaws.com", text: $endpoint)
                row(String(localized: "profile.add.s3.region"), placeholder: "us-east-1", text: $region)
                row(String(localized: "profile.add.s3.bucket"), placeholder: "my-bucket", text: $bucket)
                row(String(localized: "profile.add.s3.basePath"), placeholder: basePathDefault, text: $basePath)
                row(String(localized: "profile.add.s3.accessKeyID"), placeholder: "AKIA...", text: $accessKey)
                GridRow {
                    Text(String(localized: "profile.add.s3.secretKey")).gridColumnAlignment(.trailing).foregroundStyle(.secondary)
                    SecureField("", text: $secretKey).textFieldStyle(.roundedBorder)
                }
                GridRow {
                    Text(String(localized: "profile.add.s3.pathStyle")).gridColumnAlignment(.trailing).foregroundStyle(.secondary)
                    Toggle("", isOn: pathStyleBinding)
                        .labelsHidden()
                }
            }

            Text(String(localized: "profile.add.s3.pathStyle.hint"))
                .font(.caption)
                .foregroundStyle(.secondary)

            if let verifyMessage {
                Text(verifyMessage).font(.callout).foregroundStyle(.secondary)
            }
            if let saveError {
                Text(saveError).font(.callout).foregroundStyle(.red)
            }

            HStack {
                Button(verifying ? String(localized: "profile.add.s3.verifying") : String(localized: "profile.add.s3.verify")) { verify() }
                    .disabled(!hasMinimumFields || verifying || saving)
                Spacer()
                Button(String(localized: "common.cancel")) { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button(saving ? String(localized: "common.saving") : String(localized: "common.save")) { commit() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(!hasMinimumFields || saving || verifying)
            }
        }
        .padding(20)
        .frame(width: 540)
    }

    @ViewBuilder
    private func row(_ label: String, placeholder: String, text: Binding<String>) -> some View {
        GridRow {
            Text(label).gridColumnAlignment(.trailing).foregroundStyle(.secondary)
            TextField(placeholder, text: text)
                .textFieldStyle(.roundedBorder)
        }
    }

    private var pathStyleBinding: Binding<Bool> {
        Binding(
            get: { pathStyleOverride ?? S3Client.defaultPathStyle(forHost: S3Client.parseEndpoint(endpoint)?.host ?? "") },
            set: { pathStyleOverride = $0 }
        )
    }

    private var hasMinimumFields: Bool {
        !endpoint.trimmed.isEmpty && !bucket.trimmed.isEmpty && !accessKey.trimmed.isEmpty && !secretKey.isEmpty
    }

    private func verify() {
        verifyMessage = nil
        saveError = nil
        guard let draft = resolveDraft() else {
            saveError = String(localized: "profile.add.s3.error.invalidEndpoint")
            return
        }
        guard let probe = makeProbeRecord(from: draft) else {
            saveError = String(localized: "profile.add.s3.error.invalidEndpoint")
            return
        }
        verifying = true
        Task { [storageClientFactory, secretKey] in
            do {
                let client = try storageClientFactory.makeClient(profile: probe, password: secretKey)
                try await S3ProfileVerifier.run(
                    client: client,
                    writeAccessMessageTemplate: String(localized: "profile.add.s3.error.writeAccess")
                )
                await MainActor.run {
                    verifyMessage = String(localized: "profile.add.s3.verify.success")
                    verifying = false
                }
            } catch is CancellationError {
                await MainActor.run { verifying = false }
            } catch {
                let message = error.localizedDescription
                await MainActor.run {
                    saveError = message
                    verifying = false
                }
            }
        }
    }

    private func commit() {
        verifyMessage = nil
        saveError = nil
        guard let draft = resolveDraft(), let probe = makeProbeRecord(from: draft) else {
            saveError = String(localized: "profile.add.s3.error.invalidEndpoint")
            return
        }
        let snapshot = makeSnapshot(from: draft)
        saving = true
        Task { [storageClientFactory, secretKey, save] in
            do {
                let client = try storageClientFactory.makeClient(profile: probe, password: secretKey)
                try await S3ProfileVerifier.run(
                    client: client,
                    writeAccessMessageTemplate: String(localized: "profile.add.s3.error.writeAccess")
                )
            } catch is CancellationError {
                await MainActor.run { saving = false }
                return
            } catch {
                let message = error.localizedDescription
                await MainActor.run {
                    saveError = message
                    saving = false
                }
                return
            }
            await MainActor.run {
                do {
                    try save(snapshot)
                    saving = false
                    dismiss()
                } catch {
                    saveError = error.localizedDescription
                    saving = false
                }
            }
        }
    }

    private struct ResolvedDraft {
        let scheme: String
        let host: String
        let port: Int
        let usePathStyle: Bool
        let region: String
        let trimmedBucket: String
        let trimmedName: String
        let trimmedAccessKey: String
        let trimmedBasePath: String
    }

    private func resolveDraft() -> ResolvedDraft? {
        guard let parsed = S3Client.parseEndpoint(endpoint) else { return nil }
        return ResolvedDraft(
            scheme: parsed.scheme,
            host: parsed.host,
            port: parsed.port,
            usePathStyle: pathStyleOverride ?? S3Client.defaultPathStyle(forHost: parsed.host),
            region: S3Client.resolveRegion(userInput: region, host: parsed.host),
            trimmedBucket: bucket.trimmed,
            trimmedName: name.trimmed,
            trimmedAccessKey: accessKey.trimmed,
            trimmedBasePath: basePath.trimmed.isEmpty ? "/" : basePath.trimmed
        )
    }

    private func makeSnapshot(from draft: ResolvedDraft) -> S3ProfileSnapshot {
        S3ProfileSnapshot(
            name: draft.trimmedName,
            scheme: draft.scheme,
            host: draft.host,
            port: draft.port,
            region: draft.region,
            bucket: draft.trimmedBucket,
            basePath: draft.trimmedBasePath,
            usePathStyle: draft.usePathStyle,
            accessKeyID: draft.trimmedAccessKey,
            secretAccessKey: secretKey
        )
    }

    private func makeProbeRecord(from draft: ResolvedDraft) -> ServerProfileRecord? {
        let params = S3ConnectionParams(scheme: draft.scheme, region: draft.region, usePathStyle: draft.usePathStyle)
        guard let encoded = try? ServerProfileRecord.encodedConnectionParams(params) else { return nil }
        return ServerProfileRecord(
            id: nil,
            name: draft.trimmedName.isEmpty ? draft.trimmedBucket : draft.trimmedName,
            storageType: StorageType.s3.rawValue,
            connectionParams: encoded,
            sortOrder: 0,
            host: draft.host,
            port: draft.port,
            shareName: draft.trimmedBucket,
            basePath: RemotePathBuilder.normalizePath(draft.trimmedBasePath),
            username: draft.trimmedAccessKey,
            domain: nil,
            credentialRef: "",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
    }
}
