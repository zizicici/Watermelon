import CryptoKit
import Foundation

struct BrowserLinkSignalCipher: Sendable {
    enum Role {
        case phone
        case browser
    }

    private let sendKey: SymmetricKey
    private let receiveKey: SymmetricKey
    private let sendAdditionalData: Data
    private let receiveAdditionalData: Data

    init(pairing: BrowserLinkPairing, role: Role = .phone) {
        let salt = Data(pairing.sessionID.utf8)
        let sendDirection = role == .phone ? "phone-to-browser" : "browser-to-phone"
        let receiveDirection = role == .phone ? "browser-to-phone" : "phone-to-browser"
        sendKey = HKDF<SHA256>.deriveKey(
            inputKeyMaterial: SymmetricKey(data: pairing.secret),
            salt: salt,
            info: Data("watermelon-link-signaling-v1:\(sendDirection)".utf8),
            outputByteCount: 32
        )
        receiveKey = HKDF<SHA256>.deriveKey(
            inputKeyMaterial: SymmetricKey(data: pairing.secret),
            salt: salt,
            info: Data("watermelon-link-signaling-v1:\(receiveDirection)".utf8),
            outputByteCount: 32
        )
        sendAdditionalData = Data("watermelon-link-v1:\(pairing.sessionID):\(sendDirection)".utf8)
        receiveAdditionalData = Data("watermelon-link-v1:\(pairing.sessionID):\(receiveDirection)".utf8)
    }

    func encrypt(_ value: [String: Any]) throws -> String {
        let plaintext = try JSONSerialization.data(withJSONObject: value)
        let nonce = AES.GCM.Nonce()
        return try encrypt(plaintext, nonce: nonce)
    }

    func encrypt(_ value: [String: Any], nonceData: Data) throws -> String {
        let plaintext = try JSONSerialization.data(withJSONObject: value)
        let nonce = try AES.GCM.Nonce(data: nonceData)
        return try encrypt(plaintext, nonce: nonce)
    }

    private func encrypt(_ plaintext: Data, nonce: AES.GCM.Nonce) throws -> String {
        let box = try AES.GCM.seal(plaintext, using: sendKey, nonce: nonce, authenticating: sendAdditionalData)
        var encrypted = box.ciphertext
        encrypted.append(box.tag)
        return "\(Data(nonce).base64URLEncodedString()).\(encrypted.base64URLEncodedString())"
    }

    func decrypt(_ value: String) throws -> [String: Any] {
        let components = value.split(separator: ".", omittingEmptySubsequences: false)
        guard components.count == 2,
              let nonceData = Data(base64URLEncoded: String(components[0])),
              let encrypted = Data(base64URLEncoded: String(components[1])),
              nonceData.count == 12,
              encrypted.count >= 16 else {
            throw BrowserLinkClientError.invalidSignal
        }
        let nonce = try AES.GCM.Nonce(data: nonceData)
        let box = try AES.GCM.SealedBox(
            nonce: nonce,
            ciphertext: encrypted.dropLast(16),
            tag: encrypted.suffix(16)
        )
        let plaintext = try AES.GCM.open(box, using: receiveKey, authenticating: receiveAdditionalData)
        guard let object = try JSONSerialization.jsonObject(with: plaintext) as? [String: Any] else {
            throw BrowserLinkClientError.invalidSignal
        }
        return object
    }

    static func authenticationMAC(secret: Data, sessionID: String, nonce: String) -> String {
        let message = Data("watermelon-link-auth-v1:\(sessionID):phone-to-browser:\(nonce)".utf8)
        let code = HMAC<SHA256>.authenticationCode(for: message, using: SymmetricKey(data: secret))
        return Data(code).base64URLEncodedString()
    }

    static func authenticationConfirmationMAC(
        secret: Data,
        sessionID: String,
        nonce: String,
        folderName: String,
        browserNodeID: String,
        reclaimBrowserNodeIDs: [String],
        uploadChunkBytes: Int
    ) -> String {
        let reclaimScopes = reclaimBrowserNodeIDs.joined(separator: ",")
        let message = Data("watermelon-link-auth-v1:\(sessionID):browser-to-phone:\(nonce):\(folderName):\(browserNodeID):\(reclaimScopes):\(uploadChunkBytes)".utf8)
        let code = HMAC<SHA256>.authenticationCode(for: message, using: SymmetricKey(data: secret))
        return Data(code).base64URLEncodedString()
    }
}
