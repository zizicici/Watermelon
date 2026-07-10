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
    static let defaultService = "com.zizicici.watermelon.credentials"
    private static let accessibility = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    private let service: String

    init(service: String = KeychainService.defaultService) {
        self.service = service
    }

    func save(password: String, account: String) throws {
        let data = Data(password.utf8)
        try save(data: data, account: account)
    }

    func save(data: Data, account: String) throws {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
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
                kSecAttrService as String: service,
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
        let data = try readData(account: account)
        guard let password = String(data: data, encoding: .utf8) else {
            throw KeychainError.unexpectedData
        }
        return password
    }

    func readData(account: String) throws -> Data {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne
        ]

        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        guard status == errSecSuccess else {
            throw KeychainError.unhandled(status: status)
        }

        guard let data = result as? Data else {
            throw KeychainError.unexpectedData
        }
        return data
    }

    func readDataByAccountPrefix(_ prefix: String) throws -> [String: Data] {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecReturnAttributes as String: true,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitAll
        ]

        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        if status == errSecItemNotFound { return [:] }
        guard status == errSecSuccess else {
            throw KeychainError.unhandled(status: status)
        }
        guard let items = result as? [[String: Any]] else {
            throw KeychainError.unexpectedData
        }

        var values: [String: Data] = [:]
        for item in items {
            guard let account = item[kSecAttrAccount as String] as? String,
                  account.hasPrefix(prefix) else {
                continue
            }
            guard let data = item[kSecValueData as String] as? Data else {
                throw KeychainError.unexpectedData
            }
            values[account] = data
        }
        return values
    }

    func delete(account: String) throws {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account
        ]

        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else {
            throw KeychainError.unhandled(status: status)
        }
    }
}

extension KeychainService: @unchecked Sendable {}
