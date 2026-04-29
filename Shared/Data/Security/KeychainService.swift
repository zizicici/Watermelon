import Foundation
import Security

enum KeychainError: Error {
    case unexpectedData
    case unhandled(status: OSStatus)
}

extension KeychainError: LocalizedError {
    var errorDescription: String? {
        switch self {
        case .unexpectedData:
            return String(localized: "keychain.error.unexpectedPasswordData")
        case .unhandled(let status):
            let message = SecCopyErrorMessageString(status, nil) as String?
            return message ?? String(
                format: String(localized: "keychain.error.unhandledStatus"),
                Int(status)
            )
        }
    }
}

final class KeychainService {
    static let service = "com.zizicici.watermelon.credentials"
    private static let accessibility = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly

    func save(password: String, account: String) throws {
        let data = Data(password.utf8)
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: Self.service,
            kSecAttrAccount as String: account,
            kSecAttrAccessible as String: Self.accessibility,
            kSecValueData as String: data
        ]

        let status = SecItemAdd(query as CFDictionary, nil)
        if status == errSecDuplicateItem {
            let attributes: [String: Any] = [
                kSecValueData as String: data,
                kSecAttrAccessible as String: Self.accessibility
            ]
            let updateQuery: [String: Any] = [
                kSecClass as String: kSecClassGenericPassword,
                kSecAttrService as String: Self.service,
                kSecAttrAccount as String: account
            ]
            let updateStatus = SecItemUpdate(updateQuery as CFDictionary, attributes as CFDictionary)
            guard updateStatus == errSecSuccess else {
                throw KeychainError.unhandled(status: updateStatus)
            }
            return
        }

        guard status == errSecSuccess else {
            throw KeychainError.unhandled(status: status)
        }
    }

    func readPassword(account: String) throws -> String {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: Self.service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne
        ]

        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        guard status == errSecSuccess else {
            throw KeychainError.unhandled(status: status)
        }

        guard let data = result as? Data, let password = String(data: data, encoding: .utf8) else {
            throw KeychainError.unexpectedData
        }
        return password
    }

    func delete(account: String) throws {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: Self.service,
            kSecAttrAccount as String: account
        ]

        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else {
            throw KeychainError.unhandled(status: status)
        }
    }
}
