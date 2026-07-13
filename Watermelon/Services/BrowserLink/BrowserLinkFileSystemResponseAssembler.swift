import Foundation

enum BrowserLinkFileSystemResponseAssemblerError: Error, Equatable {
    case invalidPart
}

struct BrowserLinkFileSystemResponseAssembler {
    static let maximumPartCount = 1_366
    static let maximumPartBytes = 24 * 1024
    static let maximumResponseBytes = 32 * 1024 * 1024

    private var expectedPartCount: Int?
    private var parts: [Int: Data] = [:]
    private var byteCount = 0

    var assembledByteCount: Int { byteCount }

    mutating func append(index: Int, total: Int, part: Data) throws -> Data? {
        guard (1 ... Self.maximumPartCount).contains(total),
              (0 ..< total).contains(index),
              part.count <= Self.maximumPartBytes,
              (expectedPartCount == nil || expectedPartCount == total),
              parts[index] == nil,
              byteCount + part.count <= Self.maximumResponseBytes else {
            throw BrowserLinkFileSystemResponseAssemblerError.invalidPart
        }
        expectedPartCount = total
        parts[index] = part
        byteCount += part.count
        guard parts.count == total else { return nil }
        var result = Data(capacity: byteCount)
        for partIndex in 0 ..< total {
            guard let value = parts[partIndex] else {
                throw BrowserLinkFileSystemResponseAssemblerError.invalidPart
            }
            result.append(value)
        }
        return result
    }
}
