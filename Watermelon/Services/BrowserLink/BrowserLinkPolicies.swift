import Foundation

enum BrowserLinkProtocol {
    static let version = 1
}

struct BrowserLinkAuthenticationGate {
    private enum Phase {
        case awaitingChallenge
        case awaitingConfirmation
        case authenticated
    }

    private var phase: Phase = .awaitingChallenge

    mutating func acceptChallenge() -> Bool {
        guard phase == .awaitingChallenge else { return false }
        phase = .awaitingConfirmation
        return true
    }

    mutating func acceptConfirmation() -> Bool {
        guard phase == .awaitingConfirmation else { return false }
        phase = .authenticated
        return true
    }
}

enum BrowserLinkStartBlockReason: Equatable {
    case existingConnection
    case busy
}

enum BrowserLinkStartPolicy {
    static func blockReason(
        isConnected: Bool,
        isConnecting: Bool,
        canInteractWithRemoteNode: Bool
    ) -> BrowserLinkStartBlockReason? {
        if isConnected { return .existingConnection }
        if isConnecting || !canInteractWithRemoteNode { return .busy }
        return nil
    }
}

enum BrowserLinkNetworkPathPolicy {
    static func allowsLocalTransport(
        isSatisfied: Bool,
        usesWiFi: Bool,
        usesWiredEthernet: Bool,
        usesOther: Bool
    ) -> Bool {
        isSatisfied && (usesWiFi || usesWiredEthernet) && !usesOther
    }
}

enum BrowserLinkTransferRatePolicy {
    static let freeBytesPerSecond = 1 * 1024 * 1024

    static func maximumBytesPerSecond(rateLimitEnabled: Bool) -> Int? {
        rateLimitEnabled ? freeBytesPerSecond : nil
    }
}

struct BrowserLinkTransferRateLimiter {
    private let maximumBytesPerSecond: Int?
    private var nextPermitTime: TimeInterval?

    init(maximumBytesPerSecond: Int?) {
        self.maximumBytesPerSecond = maximumBytesPerSecond
    }

    mutating func delay(byteCount: Int, now: TimeInterval) -> TimeInterval {
        guard byteCount > 0,
              let maximumBytesPerSecond,
              maximumBytesPerSecond > 0 else { return 0 }
        let start = max(now, nextPermitTime ?? now)
        let finish = start + Double(byteCount) / Double(maximumBytesPerSecond)
        nextPermitTime = finish
        return finish - now
    }
}

enum BrowserLinkDownloadReceivePolicy {
    static let maximumTransferBytes: Int64 = 64 * 1024 * 1024 * 1024
    static let maximumUnacknowledgedBytes: Int64 = 4 * 1024 * 1024
    static let minimumNonfinalPayloadBytes = 8 * 1024

    static func nextReceivedSize(
        expectedSize: Int64,
        receivedSize: Int64,
        acknowledgedSize: Int64,
        totalUnacknowledgedBytes: Int64,
        frameOffset: Int64,
        payloadSize: Int
    ) throws -> Int64 {
        let nextSize = receivedSize + Int64(payloadSize)
        guard expectedSize >= 0,
              expectedSize <= maximumTransferBytes,
              acknowledgedSize >= 0,
              acknowledgedSize <= receivedSize,
              totalUnacknowledgedBytes >= receivedSize - acknowledgedSize,
              frameOffset == receivedSize,
              payloadSize > 0,
              payloadSize <= BrowserLinkFileFrameCodec.maximumPayloadBytes,
              frameOffset <= expectedSize,
              Int64(payloadSize) <= expectedSize - frameOffset,
              payloadSize >= minimumNonfinalPayloadBytes || nextSize == expectedSize,
              totalUnacknowledgedBytes + Int64(payloadSize) <= maximumUnacknowledgedBytes else {
            throw BrowserLinkFileFrameError.invalidFrame
        }
        return nextSize
    }
}

enum BrowserLinkDownloadAdmissionDecision: Equatable {
    case accepted
    case invalidSize
    case insufficientCapacity
}

enum BrowserLinkDownloadAdmissionPolicy {
    static let maximumRepositoryDatabaseBytes: Int64 = 256 * 1024 * 1024
    static let maximumLockBytes: Int64 = 1024 * 1024
    static let reservedLocalCapacityBytes: Int64 = 128 * 1024 * 1024

    static func maximumBytes(forRemotePath path: String) -> Int64 {
        if path.range(
            of: #"^/\.watermelon/locks/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.lock$"#,
            options: .regularExpression
        ) != nil {
            return maximumLockBytes
        }
        if path.hasSuffix("/.watermelon_manifest.sqlite") ||
            (path.contains("/.watermelon/") && path.hasSuffix(".sqlite")) {
            return maximumRepositoryDatabaseBytes
        }
        return BrowserLinkDownloadReceivePolicy.maximumTransferBytes
    }

    static func decision(
        size: Int64,
        expectedSize: Int64?,
        availableCapacity: Int64?,
        reservedCapacity: Int64 = 0,
        remotePath: String
    ) -> BrowserLinkDownloadAdmissionDecision {
        guard size >= 0,
              reservedCapacity >= 0,
              size <= maximumBytes(forRemotePath: remotePath),
              expectedSize.map({ $0 >= 0 && $0 == size }) ?? true else {
            return .invalidSize
        }
        if let availableCapacity,
           (availableCapacity < reservedLocalCapacityBytes ||
            reservedCapacity > availableCapacity - reservedLocalCapacityBytes ||
            size > availableCapacity - reservedLocalCapacityBytes - reservedCapacity) {
            return .insufficientCapacity
        }
        return .accepted
    }
}

enum BrowserLinkIngressError: Error {
    case invalidSequence
}

struct BrowserLinkOrderedIngressBuffer<Element> {
    private var nextSequence: UInt64 = 0
    private var pending: [UInt64: Element] = [:]

    mutating func insert(sequence: UInt64, value: Element, maximumPending: Int) throws -> [Element] {
        guard sequence >= nextSequence,
              pending[sequence] == nil,
              pending.count < maximumPending else {
            throw BrowserLinkIngressError.invalidSequence
        }
        pending[sequence] = value
        var ready: [Element] = []
        while let value = pending.removeValue(forKey: nextSequence) {
            ready.append(value)
            nextSequence &+= 1
        }
        return ready
    }

    mutating func removeAll() {
        pending.removeAll()
    }
}
