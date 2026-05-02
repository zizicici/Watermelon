import CryptoKit
import Foundation

enum S3SigV4Signer {
    enum BodyHash {
        case empty
        case unsigned
        case data(Data)
        case hex(String)
    }

    struct SignedRequest {
        let headers: [String: String]
        let signature: String
        let canonicalRequest: String
        let stringToSign: String
        let amzDate: String
        let payloadHash: String
    }

    static func sign(
        method: String,
        url: URL,
        additionalHeaders: [String: String] = [:],
        bodyHash: BodyHash,
        accessKeyID: String,
        secretAccessKey: String,
        sessionToken: String? = nil,
        region: String,
        service: String = "s3",
        date: Date = Date()
    ) -> SignedRequest {
        let amzDate = amzDateFormatter.string(from: date)
        let dateStamp = String(amzDate.prefix(8))

        let payloadHash = hashString(for: bodyHash)

        var headers: [String: String] = [:]
        for (key, value) in additionalHeaders {
            headers[key.lowercased()] = value
        }
        headers["host"] = canonicalHost(for: url)
        headers["x-amz-date"] = amzDate
        headers["x-amz-content-sha256"] = payloadHash
        if let sessionToken {
            headers["x-amz-security-token"] = sessionToken
        }

        let canonical = canonicalHeaders(headers)
        let canonicalURI = canonicalURIPath(for: url)
        let canonicalQuery = canonicalQueryString(for: url)

        let canonicalRequest = [
            method.uppercased(),
            canonicalURI,
            canonicalQuery,
            canonical.headersString,
            canonical.signedHeaders,
            payloadHash
        ].joined(separator: "\n")

        let canonicalRequestHash = sha256Hex(string: canonicalRequest)
        let credentialScope = "\(dateStamp)/\(region)/\(service)/aws4_request"
        let stringToSign = [
            "AWS4-HMAC-SHA256",
            amzDate,
            credentialScope,
            canonicalRequestHash
        ].joined(separator: "\n")

        let kDate = hmacSHA256(key: Data(("AWS4" + secretAccessKey).utf8), data: Data(dateStamp.utf8))
        let kRegion = hmacSHA256(key: kDate, data: Data(region.utf8))
        let kService = hmacSHA256(key: kRegion, data: Data(service.utf8))
        let kSigning = hmacSHA256(key: kService, data: Data("aws4_request".utf8))
        let signature = hexString(hmacSHA256(key: kSigning, data: Data(stringToSign.utf8)))

        let authorization = "AWS4-HMAC-SHA256 Credential=\(accessKeyID)/\(credentialScope),SignedHeaders=\(canonical.signedHeaders),Signature=\(signature)"
        headers["authorization"] = authorization

        return SignedRequest(
            headers: headers,
            signature: signature,
            canonicalRequest: canonicalRequest,
            stringToSign: stringToSign,
            amzDate: amzDate,
            payloadHash: payloadHash
        )
    }

    static let emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    static func hashString(for bodyHash: BodyHash) -> String {
        switch bodyHash {
        case .empty:
            return emptyPayloadSHA256
        case .unsigned:
            return "UNSIGNED-PAYLOAD"
        case .data(let data):
            return sha256Hex(data: data)
        case .hex(let hex):
            return hex
        }
    }

    static func sha256Hex(string: String) -> String {
        sha256Hex(data: Data(string.utf8))
    }

    static func sha256Hex(data: Data) -> String {
        hexString(Data(SHA256.hash(data: data)))
    }

    static func sha256Hex(streamingFrom url: URL) throws -> String {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var hasher = SHA256()
        while true {
            let chunk = try handle.read(upToCount: streamHashChunkSize) ?? Data()
            if chunk.isEmpty { break }
            hasher.update(data: chunk)
        }
        return hexString(Data(hasher.finalize()))
    }

    private static let streamHashChunkSize = 8 * 1024 * 1024

    static func hmacSHA256(key: Data, data: Data) -> Data {
        let symKey = SymmetricKey(data: key)
        return Data(HMAC<SHA256>.authenticationCode(for: data, using: symKey))
    }

    static func hexString(_ data: Data) -> String {
        data.map { String(format: "%02x", $0) }.joined()
    }

    private static let amzDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
        return formatter
    }()

    private static func canonicalHost(for url: URL) -> String {
        guard let host = url.host else { return "" }
        if let port = url.port {
            let scheme = url.scheme?.lowercased()
            let isDefault = (scheme == "https" && port == 443) || (scheme == "http" && port == 80)
            if !isDefault {
                return "\(host):\(port)"
            }
        }
        return host
    }

    private static func canonicalURIPath(for url: URL) -> String {
        let comps = URLComponents(url: url, resolvingAgainstBaseURL: false)
        let decodedPath = comps?.path ?? url.path
        let path = decodedPath.isEmpty ? "/" : decodedPath
        let segments = path.split(separator: "/", omittingEmptySubsequences: false).map(String.init)
        return segments.map { uriEncode($0, allowSlash: false) }.joined(separator: "/")
    }

    private static func canonicalQueryString(for url: URL) -> String {
        guard let comps = URLComponents(url: url, resolvingAgainstBaseURL: false),
              let items = comps.queryItems, !items.isEmpty else {
            return ""
        }
        let encoded: [(String, String)] = items.map { item in
            (uriEncode(item.name, allowSlash: false), uriEncode(item.value ?? "", allowSlash: false))
        }
        let sorted = encoded.sorted { lhs, rhs in
            if lhs.0 != rhs.0 { return lhs.0 < rhs.0 }
            return lhs.1 < rhs.1
        }
        return sorted.map { "\($0.0)=\($0.1)" }.joined(separator: "&")
    }

    private struct CanonicalHeaderInfo {
        let headersString: String
        let signedHeaders: String
    }

    private static func canonicalHeaders(_ headers: [String: String]) -> CanonicalHeaderInfo {
        let normalized = headers.map { (key: $0.key.lowercased(), value: trimWhitespace($0.value)) }
        let sorted = normalized.sorted { $0.key < $1.key }
        let headersString = sorted.map { "\($0.key):\($0.value)\n" }.joined()
        let signed = sorted.map { $0.key }.joined(separator: ";")
        return CanonicalHeaderInfo(headersString: headersString, signedHeaders: signed)
    }

    private static func trimWhitespace(_ value: String) -> String {
        let trimmed = value.trimmingCharacters(in: .whitespaces)
        var result = ""
        var lastWasSpace = false
        for char in trimmed {
            if char.isWhitespace {
                if !lastWasSpace {
                    result.append(" ")
                    lastWasSpace = true
                }
            } else {
                result.append(char)
                lastWasSpace = false
            }
        }
        return result
    }

    private static func uriEncode(_ value: String, allowSlash: Bool) -> String {
        let allowed = allowSlash ? unreservedWithSlash : unreserved
        return value.addingPercentEncoding(withAllowedCharacters: allowed) ?? ""
    }

    private static let unreserved: CharacterSet = {
        var set = CharacterSet()
        set.insert(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~")
        return set
    }()

    private static let unreservedWithSlash: CharacterSet = {
        var set = unreserved
        set.insert(charactersIn: "/")
        return set
    }()
}
