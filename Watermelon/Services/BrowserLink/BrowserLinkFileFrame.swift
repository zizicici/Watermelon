import Foundation

enum BrowserLinkFileFrameError: Error {
    case invalidFrame
}

enum BrowserLinkFileFrameKind: UInt8 {
    case upload = 1
    case download = 2
}

struct BrowserLinkFileFrame: Equatable {
    let transferID: String
    let offset: Int64
    let payload: Data
}

enum BrowserLinkFileFrameCodec {
    static let headerSize = 32
    static let maximumPayloadBytes = 128 * 1024
    private static let prefix: [UInt8] = [0x57, 0x4d, 0x4c]

    static func encode(
        kind: BrowserLinkFileFrameKind,
        transferID: String,
        offset: Int64,
        payload: Data
    ) throws -> Data {
        guard let identifier = UUID(uuidString: transferID),
              identifier.uuidString.lowercased() == transferID,
              offset >= 0,
              !payload.isEmpty,
              payload.count <= maximumPayloadBytes else {
            throw BrowserLinkFileFrameError.invalidFrame
        }
        var frame = Data(capacity: headerSize + payload.count)
        frame.append(contentsOf: prefix)
        frame.append(kind.rawValue)
        var uuid = identifier.uuid
        withUnsafeBytes(of: &uuid) { frame.append(contentsOf: $0) }
        append(UInt64(offset), to: &frame)
        append(UInt32(payload.count), to: &frame)
        frame.append(payload)
        return frame
    }

    static func decode(_ data: Data, expectedKind: BrowserLinkFileFrameKind) throws -> BrowserLinkFileFrame {
        guard data.count > headerSize,
              data.count <= headerSize + maximumPayloadBytes,
              Array(data.prefix(4)) == prefix + [expectedKind.rawValue] else {
            throw BrowserLinkFileFrameError.invalidFrame
        }
        let identifierBytes = Array(data[4..<20])
        let identifier = UUID(uuid: (
            identifierBytes[0], identifierBytes[1], identifierBytes[2], identifierBytes[3],
            identifierBytes[4], identifierBytes[5], identifierBytes[6], identifierBytes[7],
            identifierBytes[8], identifierBytes[9], identifierBytes[10], identifierBytes[11],
            identifierBytes[12], identifierBytes[13], identifierBytes[14], identifierBytes[15]
        ))
        let encodedOffset = unsignedInteger(data[20..<28])
        let payloadLength = unsignedInteger(data[28..<32])
        guard encodedOffset <= UInt64(Int64.max),
              payloadLength > 0,
              payloadLength <= UInt64(maximumPayloadBytes),
              data.count == headerSize + Int(payloadLength) else {
            throw BrowserLinkFileFrameError.invalidFrame
        }
        return BrowserLinkFileFrame(
            transferID: identifier.uuidString.lowercased(),
            offset: Int64(encodedOffset),
            payload: data.dropFirst(headerSize)
        )
    }

    private static func unsignedInteger(_ bytes: Data.SubSequence) -> UInt64 {
        bytes.reduce(UInt64.zero) { ($0 << 8) | UInt64($1) }
    }

    private static func append(_ value: UInt64, to data: inout Data) {
        var bigEndian = value.bigEndian
        withUnsafeBytes(of: &bigEndian) { data.append(contentsOf: $0) }
    }

    private static func append(_ value: UInt32, to data: inout Data) {
        var bigEndian = value.bigEndian
        withUnsafeBytes(of: &bigEndian) { data.append(contentsOf: $0) }
    }
}
