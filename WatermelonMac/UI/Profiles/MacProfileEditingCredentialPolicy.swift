import Foundation

enum MacProfileEditingCredentialPolicy {
    static func plainCredential(
        storedCredential: String?
    ) -> String {
        storedCredential ?? ""
    }

    static func sftpCredential(
        storedCredential: String?,
        authMethod: SFTPConnectionParams.AuthMethod
    ) -> SFTPCredentialBlob? {
        guard let storedCredential,
              let credential = try? SFTPCredentialBlob.decode(
                from: storedCredential
              ) else {
            return nil
        }
        switch (credential, authMethod) {
        case (.password, .password),
             (.privateKey, .privateKey):
            return credential
        default:
            return nil
        }
    }
}
