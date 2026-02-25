import Foundation
import Security

enum KeychainError: Error {
    case unexpectedData
    case unhandled(status: OSStatus)
}

final class KeychainService {
    static let service = "com.zizicici.photobackup.credentials"

    func save(password: String, account: String) throws {
        let data = Data(password.utf8)
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: Self.service,
            kSecAttrAccount as String: account,
            kSecValueData as String: data
        ]

        let status = SecItemAdd(query as CFDictionary, nil)
        if status == errSecDuplicateItem {
            let attributes = [kSecValueData as String: data]
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
