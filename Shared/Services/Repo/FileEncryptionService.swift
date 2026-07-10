import CryptoKit
import Foundation
import Security

enum FileEncryptionError: Error, Equatable {
    case invalidHeader
    case unsupportedFormat
    case metadataTooLarge
    case invalidKey
    case authenticationFailed
    case truncatedCiphertext
    case trailingCiphertext
    case randomGenerationFailed
}

struct FileEncryptionMetadata: Codable, Equatable, Sendable {
    var originalFileName: String
    var resourceType: Int? = nil
    var creationDateMs: Int64? = nil
    var plainSHA256: String? = nil
    var plainSize: Int64? = nil

    enum CodingKeys: String, CodingKey {
        case originalFileName = "original_file_name"
        case resourceType = "resource_type"
        case creationDateMs = "creation_date_ms"
        case plainSHA256 = "plain_sha256"
        case plainSize = "plain_size"
    }
}

struct FileEncryptionService: Sendable {
    private static let magic = Data([0x57, 0x4d, 0x45, 0x4e, 0x43, 0x31, 0x00, 0x00])
    private static let version = 1
    private static let algorithm = "AES-256-GCM-HKDF-SHA256"
    private static let fileKeyInfo = Data("Watermelon file encryption v1".utf8)
    private static let chunkSize = 4 * 1024 * 1024
    private static let maxHeaderLength = 16 * 1024
    private static let maxPlainSize: Int64 = 4 * 1024 * 1024 * 1024 * 1024
    private static let maxContentRecordCount = 1_048_576
    private static let metadataBuckets = [1024, 4096, 16384, 65536]
    private static let tagSize = 16

    static func resourceExternalAAD(contentHash: Data) -> Data {
        var data = Data("Watermelon resource file v1\n".utf8)
        data.append(contentHash)
        return data
    }

    static func thumbnailExternalAAD(fingerprintHex: String) -> Data {
        Data("Watermelon thumbnail sidecar v1\n\(fingerprintHex)".utf8)
    }

    private struct Header: Codable, Equatable {
        let v: Int
        let alg: String
        let kid: String
        let salt: String
        let chunkSize: Int
        let metadataRecordSize: Int
        let contentRecordCount: Int
        let plainSize: Int64

        enum CodingKeys: String, CodingKey {
            case v
            case alg
            case kid
            case salt
            case chunkSize = "chunk_size"
            case metadataRecordSize = "metadata_record_size"
            case contentRecordCount = "content_record_count"
            case plainSize = "plain_size"
        }
    }

    func encrypt(
        plaintextURL: URL,
        encryptedURL: URL,
        metadata: FileEncryptionMetadata,
        keyMaterial: RepoEncryptionKeyMaterial,
        externalAAD: Data
    ) throws {
        let plainSize = try fileSize(plaintextURL)
        guard plainSize <= Self.maxPlainSize else { throw FileEncryptionError.unsupportedFormat }
        let contentRecordCount = Self.contentRecordCount(plainSize: plainSize)
        guard contentRecordCount <= Self.maxContentRecordCount else { throw FileEncryptionError.unsupportedFormat }

        var metadataToWrite = metadata
        metadataToWrite.plainSize = plainSize
        let salt = try Self.randomBytes(count: RepoEncryptionKeyMaterial.byteCount)
        let metadataRecordSize = try Self.metadataRecordSize(for: metadataToWrite)
        let header = Header(
            v: Self.version,
            alg: Self.algorithm,
            kid: keyMaterial.keyID,
            salt: Self.base64URL(salt),
            chunkSize: Self.chunkSize,
            metadataRecordSize: metadataRecordSize,
            contentRecordCount: contentRecordCount,
            plainSize: plainSize
        )
        let headerData = try Self.encodeHeader(header)
        let fileKey = Self.deriveFileKey(repoKey: keyMaterial.keyData, salt: salt)

        try? FileManager.default.removeItem(at: encryptedURL)
        _ = FileManager.default.createFile(atPath: encryptedURL.path, contents: nil)

        let input = try FileHandle(forReadingFrom: plaintextURL)
        defer { try? input.close() }
        let output = try FileHandle(forWritingTo: encryptedURL)
        defer { try? output.close() }

        try output.write(contentsOf: Self.magic)
        try output.write(contentsOf: Self.uint32BE(UInt32(headerData.count)))
        try output.write(contentsOf: headerData)

        let metadataPlaintext = try Self.paddedMetadata(metadataToWrite, size: metadataRecordSize)
        let metadataSealed = try Self.seal(
            metadataPlaintext,
            fileKey: fileKey,
            externalAAD: externalAAD,
            headerData: headerData,
            recordIndex: 0
        )
        try output.write(contentsOf: metadataSealed)

        if plainSize == 0 {
            let sealed = try Self.seal(
                Data(),
                fileKey: fileKey,
                externalAAD: externalAAD,
                headerData: headerData,
                recordIndex: 1
            )
            try output.write(contentsOf: sealed)
            return
        }

        var recordIndex: UInt64 = 1
        while true {
            let chunk = try input.read(upToCount: Self.chunkSize) ?? Data()
            if chunk.isEmpty { break }
            let sealed = try Self.seal(
                chunk,
                fileKey: fileKey,
                externalAAD: externalAAD,
                headerData: headerData,
                recordIndex: recordIndex
            )
            try output.write(contentsOf: sealed)
            recordIndex += 1
        }
    }

    @discardableResult
    func decrypt(
        encryptedURL: URL,
        plaintextURL: URL,
        keyMaterial: RepoEncryptionKeyMaterial,
        externalAAD: Data
    ) throws -> FileEncryptionMetadata {
        let input = try FileHandle(forReadingFrom: encryptedURL)
        defer { try? input.close() }

        guard try readExact(input, count: Self.magic.count) == Self.magic,
              let headerLengthData = try readExact(input, count: 4),
              let headerLength = Self.uint32BE(headerLengthData),
              headerLength > 0,
              headerLength <= Self.maxHeaderLength,
              let headerData = try readExact(input, count: Int(headerLength)) else {
            throw FileEncryptionError.invalidHeader
        }
        let header = try Self.decodeAndValidateHeader(headerData)
        guard header.kid == keyMaterial.keyID,
              let salt = Self.base64URLDecode(header.salt),
              salt.count == RepoEncryptionKeyMaterial.byteCount else {
            throw FileEncryptionError.invalidKey
        }
        let fileKey = Self.deriveFileKey(repoKey: keyMaterial.keyData, salt: salt)

        let metadataSealedSize = header.metadataRecordSize + Self.tagSize
        guard let metadataSealed = try readExact(input, count: metadataSealedSize) else {
            throw FileEncryptionError.truncatedCiphertext
        }
        let metadataPlaintext = try Self.open(
            metadataSealed,
            plaintextSize: header.metadataRecordSize,
            fileKey: fileKey,
            externalAAD: externalAAD,
            headerData: headerData,
            recordIndex: 0
        )
        let metadata = try Self.decodeMetadata(metadataPlaintext)

        try? FileManager.default.removeItem(at: plaintextURL)
        _ = FileManager.default.createFile(atPath: plaintextURL.path, contents: nil)
        let output = try FileHandle(forWritingTo: plaintextURL)
        defer { try? output.close() }

        for recordOffset in 0..<header.contentRecordCount {
            let chunkPlaintextSize = Self.plaintextChunkSize(
                plainSize: header.plainSize,
                recordOffset: recordOffset,
                recordCount: header.contentRecordCount
            )
            let sealedSize = chunkPlaintextSize + Self.tagSize
            guard let sealed = try readExact(input, count: sealedSize) else {
                throw FileEncryptionError.truncatedCiphertext
            }
            let chunk = try Self.open(
                sealed,
                plaintextSize: chunkPlaintextSize,
                fileKey: fileKey,
                externalAAD: externalAAD,
                headerData: headerData,
                recordIndex: UInt64(recordOffset + 1)
            )
            try output.write(contentsOf: chunk)
        }
        if let trailing = try input.read(upToCount: 1), !trailing.isEmpty {
            throw FileEncryptionError.trailingCiphertext
        }
        return metadata
    }

    private static func encodeHeader(_ header: Header) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(header)
        guard data.count <= maxHeaderLength else { throw FileEncryptionError.invalidHeader }
        return data
    }

    private static func decodeAndValidateHeader(_ data: Data) throws -> Header {
        let header = try JSONDecoder().decode(Header.self, from: data)
        guard header.v == version,
              header.alg == algorithm,
              header.chunkSize == chunkSize,
              metadataBuckets.contains(header.metadataRecordSize),
              header.plainSize >= 0,
              header.plainSize <= maxPlainSize else {
            throw FileEncryptionError.unsupportedFormat
        }
        let expectedCount = contentRecordCount(plainSize: header.plainSize)
        guard header.contentRecordCount == expectedCount,
              header.contentRecordCount <= maxContentRecordCount else {
            throw FileEncryptionError.unsupportedFormat
        }
        return header
    }

    private static func metadataRecordSize(for metadata: FileEncryptionMetadata) throws -> Int {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(metadata)
        guard let bucket = metadataBuckets.first(where: { data.count <= $0 }) else {
            throw FileEncryptionError.metadataTooLarge
        }
        return bucket
    }

    private static func paddedMetadata(_ metadata: FileEncryptionMetadata, size: Int) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        var data = try encoder.encode(metadata)
        guard data.count <= size else { throw FileEncryptionError.metadataTooLarge }
        data.append(Data(repeating: 0, count: size - data.count))
        return data
    }

    private static func decodeMetadata(_ padded: Data) throws -> FileEncryptionMetadata {
        let jsonEnd = padded.firstIndex(of: 0) ?? padded.endIndex
        return try JSONDecoder().decode(FileEncryptionMetadata.self, from: padded[..<jsonEnd])
    }

    private static func seal(
        _ plaintext: Data,
        fileKey: SymmetricKey,
        externalAAD: Data,
        headerData: Data,
        recordIndex: UInt64
    ) throws -> Data {
        let nonce = try AES.GCM.Nonce(data: nonceData(recordIndex))
        let box = try AES.GCM.seal(
            plaintext,
            using: fileKey,
            nonce: nonce,
            authenticating: aad(externalAAD: externalAAD, headerData: headerData, recordIndex: recordIndex)
        )
        return box.ciphertext + box.tag
    }

    private static func open(
        _ sealed: Data,
        plaintextSize: Int,
        fileKey: SymmetricKey,
        externalAAD: Data,
        headerData: Data,
        recordIndex: UInt64
    ) throws -> Data {
        guard sealed.count == plaintextSize + tagSize else {
            throw FileEncryptionError.truncatedCiphertext
        }
        let ciphertext = sealed.prefix(plaintextSize)
        let tag = sealed.suffix(tagSize)
        let nonce = try AES.GCM.Nonce(data: nonceData(recordIndex))
        let box = try AES.GCM.SealedBox(nonce: nonce, ciphertext: ciphertext, tag: tag)
        do {
            return try AES.GCM.open(
                box,
                using: fileKey,
                authenticating: aad(externalAAD: externalAAD, headerData: headerData, recordIndex: recordIndex)
            )
        } catch {
            throw FileEncryptionError.authenticationFailed
        }
    }

    private static func deriveFileKey(repoKey: Data, salt: Data) -> SymmetricKey {
        HKDF<SHA256>.deriveKey(
            inputKeyMaterial: SymmetricKey(data: repoKey),
            salt: salt,
            info: fileKeyInfo,
            outputByteCount: RepoEncryptionKeyMaterial.byteCount
        )
    }

    private static func aad(externalAAD: Data, headerData: Data, recordIndex: UInt64) -> Data {
        externalAAD + headerData + uint64BE(recordIndex)
    }

    private static func nonceData(_ recordIndex: UInt64) -> Data {
        Data(repeating: 0, count: 4) + uint64BE(recordIndex)
    }

    private static func contentRecordCount(plainSize: Int64) -> Int {
        if plainSize == 0 { return 1 }
        return Int((plainSize + Int64(chunkSize) - 1) / Int64(chunkSize))
    }

    private static func plaintextChunkSize(plainSize: Int64, recordOffset: Int, recordCount: Int) -> Int {
        if plainSize == 0 { return 0 }
        if recordOffset < recordCount - 1 { return chunkSize }
        return Int(plainSize - Int64(chunkSize * (recordCount - 1)))
    }

    private func fileSize(_ url: URL) throws -> Int64 {
        let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
        return (attributes[.size] as? NSNumber)?.int64Value ?? 0
    }

    private func readExact(_ handle: FileHandle, count: Int) throws -> Data? {
        let data = try handle.read(upToCount: count)
        guard let data, data.count == count else { return nil }
        return data
    }

    private static func randomBytes(count: Int) throws -> Data {
        var data = Data(count: count)
        let status = data.withUnsafeMutableBytes { buffer in
            guard let baseAddress = buffer.baseAddress else { return errSecParam }
            return SecRandomCopyBytes(kSecRandomDefault, count, baseAddress)
        }
        guard status == errSecSuccess else { throw FileEncryptionError.randomGenerationFailed }
        return data
    }

    private static func uint32BE(_ value: UInt32) -> Data {
        var bigEndian = value.bigEndian
        return Data(bytes: &bigEndian, count: MemoryLayout<UInt32>.size)
    }

    private static func uint32BE(_ data: Data) -> UInt32? {
        guard data.count == MemoryLayout<UInt32>.size else { return nil }
        return data.reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
    }

    private static func uint64BE(_ value: UInt64) -> Data {
        var bigEndian = value.bigEndian
        return Data(bytes: &bigEndian, count: MemoryLayout<UInt64>.size)
    }

    private static func base64URL(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private static func base64URLDecode(_ text: String) -> Data? {
        guard !text.isEmpty else { return nil }
        var base64 = text
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        let padding = (4 - base64.count % 4) % 4
        if padding > 0 {
            base64 += String(repeating: "=", count: padding)
        }
        return Data(base64Encoded: base64)
    }
}
