import CryptoKit
import Foundation
import Network
import os
@preconcurrency import WebRTC

final class BrowserLinkIngressSequencer: @unchecked Sendable {
    private let lock = NSLock()
    private var nextSequence: UInt64 = 0
    private var pendingMessages = 0
    private var pendingBytes = 0
    private var overflowReported = false
    private var overflowNotificationClaimed = false

    func reserveSequence(messageBytes: Int, maximumMessages: Int, maximumBytes: Int) -> UInt64? {
        lock.lock()
        defer { lock.unlock() }
        guard !overflowReported,
              messageBytes >= 0,
              pendingMessages < maximumMessages,
              pendingBytes <= maximumBytes - messageBytes else {
            overflowReported = true
            return nil
        }
        let sequence = nextSequence
        nextSequence &+= 1
        pendingMessages += 1
        pendingBytes += messageBytes
        return sequence
    }

    func release(messageBytes: Int) {
        lock.lock()
        pendingMessages = max(0, pendingMessages - 1)
        pendingBytes = max(0, pendingBytes - messageBytes)
        lock.unlock()
    }

    func claimOverflowNotification() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard overflowReported, !overflowNotificationClaimed else { return false }
        overflowNotificationClaimed = true
        return true
    }

    func reset() {
        lock.lock()
        pendingMessages = 0
        pendingBytes = 0
        overflowReported = false
        overflowNotificationClaimed = false
        lock.unlock()
    }
}

private struct BrowserLinkIncomingDataMessage: @unchecked Sendable {
    let channelIdentity: ObjectIdentifier
    let data: Data
    let isBinary: Bool
}

enum BrowserLinkClientState: Equatable, Sendable {
    case connecting
    case waitingForDesktop
    case negotiating
    case authenticating
    case connected
    case failed(String)
}

enum BrowserLinkDownloadYieldDisposition: Equatable {
    case accepted
    case abandoned
    case protocolFailure
}

enum BrowserLinkClientError: LocalizedError {
    case invalidServerMessage
    case invalidSignal
    case unexpectedSignal
    case peerLeft
    case webRTCUnavailable
    case localNetworkRequired
    case authenticationFailed
    case connectionClosed
    case requestFailed

    var errorDescription: String? {
        switch self {
        case .invalidServerMessage, .invalidSignal, .unexpectedSignal:
            String(localized: "link.error.invalidSignal")
        case .peerLeft:
            String(localized: "link.error.desktopLeft")
        case .webRTCUnavailable:
            String(localized: "link.error.webRTC")
        case .localNetworkRequired:
            String(localized: "link.connection.sameNetworkHint")
        case .authenticationFailed:
            String(localized: "link.error.authentication")
        case .connectionClosed, .requestFailed:
            String(localized: "link.error.connection")
        }
    }
}

@MainActor
final class BrowserLinkClient: NSObject {
    enum FileSystemRequestPriority: Hashable {
        case ordinary
        case control
        case cleanup
    }

    var onStateChange: ((BrowserLinkClientState) -> Void)?
    var onTerminalFailure: ((Error) -> Void)?
    private(set) var remoteFolderName = ""
    private(set) var remoteBrowserNodeID: String?
    private(set) var reclaimBrowserNodeIDs: [String] = []
    private(set) var uploadChunkBytes = 32 * 1024
    var isFileSystemReady: Bool {
        state == .connected && dataChannel?.readyState == .open
    }

    private struct PendingRequest {
        let continuation: CheckedContinuation<Data, Error>
        var timeoutTask: Task<Void, Never>
        var responseAssembler = BrowserLinkFileSystemResponseAssembler()
    }

    private struct OutgoingUpload {
        let expectedSize: Int64
        let control: Bool
        var sentSize: Int64 = 0
        var acknowledgedSize: Int64 = 0
        var failure: Error?
    }

    private struct IncomingDownload {
        let expectedSize: Int64
        var receivedSize: Int64 = 0
        var acknowledgedSize: Int64 = 0
        let continuation: AsyncThrowingStream<Data, Error>.Continuation
        var idleTimeoutTask: Task<Void, Never>?
    }

    private struct RequestSlotWaiter {
        let priority: FileSystemRequestPriority
        let continuation: CheckedContinuation<Void, Error>
    }

    private let pairing: BrowserLinkPairing
    nonisolated private let ingressSequencer = BrowserLinkIngressSequencer()
    private let cipher: BrowserLinkSignalCipher
    private let peerConnectionFactory = RTCPeerConnectionFactory()
    private let pathMonitor = NWPathMonitor()
    private let pathMonitorQueue = DispatchQueue(label: "com.zizicici.watermelon.link.path")
    private var webSocket: URLSessionWebSocketTask?
    private var receiveTask: Task<Void, Never>?
    private var authenticationTimeoutTask: Task<Void, Never>?
    private var connectionTimeoutTask: Task<Void, Never>?
    private var disconnectionTimeoutTask: Task<Void, Never>?
    private var peerConnection: RTCPeerConnection?
    private var dataChannel: RTCDataChannel?
    private var pendingLocalCandidates: [RTCIceCandidate] = []
    private var authenticationGate = BrowserLinkAuthenticationGate()
    private var authenticationNonce: String?
    private var localDescriptionSent = false
    private var signalingCompleted = false
    private var state: BrowserLinkClientState = .connecting
    private var stopped = false
    private var signalingStarted = false
    private var pendingRequests: [String: PendingRequest] = [:]
    private var outgoingUploads: [String: OutgoingUpload] = [:]
    private var incomingDownloads: [String: IncomingDownload] = [:]
    private var ignoredIncomingDownloadIDs: Set<String> = []
    private var pendingDownloadAbortIDs: Set<String> = []
    private var downloadAbortCleanupTasks: [String: Task<Void, Never>] = [:]
    private var uploadFlowWaiters: [UUID: CheckedContinuation<Void, Error>] = [:]
    private var requestSlotWaiters: [UUID: RequestSlotWaiter] = [:]
    private var requestSlotWaiterOrder: [UUID] = []
    private var incomingMessageBuffer = BrowserLinkOrderedIngressBuffer<BrowserLinkIncomingDataMessage>()
    nonisolated private static let maximumPendingRequests = 8
    nonisolated private static let maximumOrdinaryPendingRequests = 6
    nonisolated private static let maximumControlPendingRequests = 7
    nonisolated private static let maximumOrdinaryRequestSlotWaiters = 12
    nonisolated private static let maximumControlRequestSlotWaiters = 15
    nonisolated private static let maximumRequestSlotWaiters = 16
    private static let maximumPendingResponseBytes = 32 * 1024 * 1024
    private static let maximumIncomingMessageBytes = BrowserLinkFileFrameCodec.headerSize + BrowserLinkFileFrameCodec.maximumPayloadBytes
    private static let maximumIncomingDownloads = 3
    private static let maximumBufferedDownloadFrames = 512
    nonisolated private static let maximumPendingIncomingMessages = 1_024
    nonisolated private static let maximumPendingIncomingMessageBytes = 8 * 1024 * 1024
    nonisolated private static let maximumUnacknowledgedUploadBytes = 4 * 1024 * 1024
    nonisolated private static let maximumDataUnacknowledgedUploadBytes = 3 * 1024 * 1024 + 512 * 1024
    nonisolated private static let maximumControlUnacknowledgedUploadBytes = 512 * 1024
    nonisolated private static let maximumDataBufferedUploadBytes = BrowserLinkFileFrameCodec.headerSize + BrowserLinkFileFrameCodec.maximumPayloadBytes
    nonisolated private static let maximumBufferedUploadBytes = 192 * 1024
    nonisolated static let uploadFlowTimeout: Duration = .seconds(65)
    nonisolated static let abandonedDownloadRetention: Duration = .seconds(305)

    init(pairing: BrowserLinkPairing) {
        self.pairing = pairing
        self.cipher = BrowserLinkSignalCipher(pairing: pairing)
        super.init()
    }

    func start() {
        guard !signalingStarted, webSocket == nil else { return }
        browserLinkLog.info("Client start")
        setState(.connecting)
        pathMonitor.pathUpdateHandler = { [weak self] path in
            let usesLocalTransport = BrowserLinkNetworkPathPolicy.allowsLocalTransport(
                isSatisfied: path.status == .satisfied,
                usesWiFi: path.usesInterfaceType(.wifi),
                usesWiredEthernet: path.usesInterfaceType(.wiredEthernet),
                usesOther: path.usesInterfaceType(.other)
            )
            browserLinkLog.info(
                "Network path status=\(String(describing: path.status), privacy: .public) wifi=\(path.usesInterfaceType(.wifi)) other=\(path.usesInterfaceType(.other)) cellular=\(path.usesInterfaceType(.cellular)) wired=\(path.usesInterfaceType(.wiredEthernet)) accepted=\(usesLocalTransport)"
            )
            Task { @MainActor [weak self] in self?.handleNetworkPath(usesLocalTransport: usesLocalTransport) }
        }
        pathMonitor.start(queue: pathMonitorQueue)
    }

    private func handleNetworkPath(usesLocalTransport: Bool) {
        guard !stopped else { return }
        guard usesLocalTransport else {
            browserLinkLog.error("Signaling rejected because a supported local network path is unavailable")
            fail(BrowserLinkClientError.localNetworkRequired)
            return
        }
        guard !signalingStarted else { return }
        signalingStarted = true
        browserLinkLog.info("Opening signaling WebSocket")
        let socket = URLSession.shared.webSocketTask(with: pairing.signalingURL)
        webSocket = socket
        socket.resume()
        connectionTimeoutTask = Task { [weak self] in
            try? await Task.sleep(for: .seconds(30))
            guard !Task.isCancelled, let self, self.state != .connected else { return }
            self.fail(BrowserLinkClientError.connectionClosed)
        }
        receiveTask = Task { [weak self] in
            await self?.receiveMessages(from: socket)
        }
    }

    func stop() {
        dispose(sendCancel: true)
    }

    private func receiveMessages(from socket: URLSessionWebSocketTask) async {
        do {
            while !Task.isCancelled, !stopped {
                let message = try await socket.receive()
                let raw: String
                switch message {
                case .string(let value): raw = value
                case .data: throw BrowserLinkClientError.invalidServerMessage
                @unknown default: throw BrowserLinkClientError.invalidServerMessage
                }
                try await handleServerMessage(raw)
                if webSocket !== socket { break }
            }
        } catch is CancellationError {
        } catch {
            browserLinkLog.error("Signaling receive failed type=\(String(reflecting: type(of: error)), privacy: .public)")
            if !stopped, state != .connected {
                fail(Self.signalingCloseError(closeReason: socket.closeReason, underlying: error))
            }
        }
    }

    nonisolated static func signalingCloseError(closeReason: Data?, underlying: Error) -> Error {
        guard closeReason.flatMap({ String(data: $0, encoding: .utf8) }) == "peer_left" else {
            return underlying
        }
        return BrowserLinkClientError.peerLeft
    }

    private func handleServerMessage(_ raw: String) async throws {
        guard let data = raw.data(using: .utf8),
              let message = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let kind = message["kind"] as? String else {
            throw BrowserLinkClientError.invalidServerMessage
        }
        switch kind {
        case "control":
            guard let event = message["event"] as? String else {
                throw BrowserLinkClientError.invalidServerMessage
            }
            browserLinkLog.info("Signaling control event=\(event, privacy: .public)")
            switch event {
            case "waiting": setState(.waitingForDesktop)
            case "peer_joined": setState(.negotiating)
            case "peer_left": throw BrowserLinkClientError.peerLeft
            case "signaling_complete":
                signalingCompleted = true
                webSocket?.cancel(with: .normalClosure, reason: nil)
                webSocket = nil
            default: throw BrowserLinkClientError.invalidServerMessage
            }
        case "relay":
            browserLinkLog.info("Encrypted signaling relay received")
            guard let payload = message["payload"] as? String else {
                throw BrowserLinkClientError.invalidServerMessage
            }
            try await handleSignal(cipher.decrypt(payload))
        case "error":
            throw BrowserLinkClientError.connectionClosed
        default:
            throw BrowserLinkClientError.invalidServerMessage
        }
    }

    private func handleSignal(_ signal: [String: Any]) async throws {
        guard let type = signal["type"] as? String else {
            throw BrowserLinkClientError.invalidSignal
        }
        switch type {
        case "offer":
            guard peerConnection == nil,
                  let description = signal["description"] as? [String: Any],
                  description["type"] as? String == "offer",
                  let sdp = description["sdp"] as? String else {
                throw BrowserLinkClientError.unexpectedSignal
            }
            let statistics = BrowserLinkICEPolicy.statistics(in: sdp)
            browserLinkLog.info("Offer received candidates=\(statistics.total) allowed=\(statistics.allowed)")
            try await answerOffer(sdp)
        case "ice":
            guard let peerConnection,
                  let value = signal["candidate"] as? [String: Any],
                  let sdp = value["candidate"] as? String,
                  let line = value["sdpMLineIndex"] as? NSNumber else {
                throw BrowserLinkClientError.invalidSignal
            }
            let candidate = RTCIceCandidate(
                sdp: sdp,
                sdpMLineIndex: line.int32Value,
                sdpMid: value["sdpMid"] as? String
            )
            let label = BrowserLinkICEPolicy.diagnosticLabel(for: candidate.sdp)
            browserLinkLog.info("Remote ICE candidate \(label, privacy: .public)")
            guard BrowserLinkICEPolicy.allows(candidateSDP: candidate.sdp) else { return }
            try await add(candidate, to: peerConnection)
        default:
            throw BrowserLinkClientError.unexpectedSignal
        }
    }

    private func answerOffer(_ sdp: String) async throws {
        let configuration = RTCConfiguration()
        configuration.iceServers = []
        configuration.sdpSemantics = .unifiedPlan
        let constraints = RTCMediaConstraints(mandatoryConstraints: nil, optionalConstraints: nil)
        guard let connection = peerConnectionFactory.peerConnection(
            with: configuration,
            constraints: constraints,
            delegate: self
        ) else {
            throw BrowserLinkClientError.webRTCUnavailable
        }
        peerConnection = connection
        try await setRemoteDescription(
            RTCSessionDescription(type: .offer, sdp: BrowserLinkICEPolicy.filteringCandidates(in: sdp)),
            on: connection
        )
        let answer = try await createAnswer(on: connection, constraints: constraints)
        try await setLocalDescription(answer, on: connection)
        let local = connection.localDescription ?? answer
        let statistics = BrowserLinkICEPolicy.statistics(in: local.sdp)
        browserLinkLog.info("Answer created candidates=\(statistics.total) allowed=\(statistics.allowed) pendingTrickle=\(self.pendingLocalCandidates.count)")
        try await sendSignal([
            "type": "answer",
            "description": ["type": "answer", "sdp": BrowserLinkICEPolicy.filteringCandidates(in: local.sdp)],
        ])
        localDescriptionSent = true
        let candidates = pendingLocalCandidates
        pendingLocalCandidates.removeAll()
        for candidate in candidates { try await send(candidate) }
    }

    private func send(_ candidate: RTCIceCandidate) async throws {
        guard !signalingCompleted else {
            browserLinkLog.info("Ignored ICE candidate after signaling completed")
            return
        }
        let label = BrowserLinkICEPolicy.diagnosticLabel(for: candidate.sdp)
        browserLinkLog.info("Local ICE candidate \(label, privacy: .public)")
        guard BrowserLinkICEPolicy.allows(candidateSDP: candidate.sdp) else { return }
        var value: [String: Any] = [
            "candidate": candidate.sdp,
            "sdpMLineIndex": Int(candidate.sdpMLineIndex),
        ]
        if let mid = candidate.sdpMid { value["sdpMid"] = mid }
        do {
            try await sendSignal(["type": "ice", "candidate": value])
        } catch {
            if signalingCompleted {
                browserLinkLog.info("Ignored in-flight ICE send after signaling completed")
                return
            }
            throw error
        }
    }

    private func sendSignal(_ signal: [String: Any]) async throws {
        guard let webSocket else { throw BrowserLinkClientError.connectionClosed }
        let payload = try cipher.encrypt(signal)
        let data = try JSONSerialization.data(withJSONObject: ["kind": "relay", "payload": payload])
        try await webSocket.send(.string(String(decoding: data, as: UTF8.self)))
    }

    private func enqueueDataChannelMessage(
        sequence: UInt64,
        message: BrowserLinkIncomingDataMessage
    ) {
        guard !stopped else {
            ingressSequencer.release(messageBytes: message.data.count)
            return
        }
        let ready: [BrowserLinkIncomingDataMessage]
        do {
            ready = try incomingMessageBuffer.insert(
                sequence: sequence,
                value: message,
                maximumPending: Self.maximumPendingIncomingMessages
            )
        } catch {
            ingressSequencer.release(messageBytes: message.data.count)
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        ready.forEach { ingressSequencer.release(messageBytes: $0.data.count) }
        for message in ready {
            guard !stopped else { return }
            guard dataChannel.map(ObjectIdentifier.init) == message.channelIdentity else { continue }
            handleDataChannelMessage(message.data, isBinary: message.isBinary)
        }
    }

    private func handleDataChannelMessage(_ data: Data, isBinary: Bool) {
        guard data.count <= Self.maximumIncomingMessageBytes,
              let dataChannel,
              dataChannel.readyState == .open else {
            failDataChannelProtocol()
            return
        }
        if isBinary {
            guard state == .connected else {
                fail(BrowserLinkClientError.authenticationFailed)
                return
            }
            handleDownloadFrame(data)
            return
        }
        guard let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let type = object["type"] as? String else {
            failDataChannelProtocol()
            return
        }
        if state == .connected {
            handleFileSystemResponse(object, type: type)
            return
        }
        if type == "auth_ok" {
            browserLinkLog.info("Data channel auth confirmation received")
            guard state == .authenticating,
                  authenticationGate.acceptConfirmation(),
                  (object["protocolVersion"] as? NSNumber)?.intValue == BrowserLinkProtocol.version,
                  let folderName = object["folderName"] as? String,
                  let browserNodeID = object["browserNodeID"] as? String,
                  Self.isCanonicalBrowserNodeID(browserNodeID),
                  let reclaimBrowserNodeIDs = object["reclaimBrowserNodeIDs"] as? [String],
                  reclaimBrowserNodeIDs.count <= 16,
                  Set(reclaimBrowserNodeIDs).count == reclaimBrowserNodeIDs.count,
                  reclaimBrowserNodeIDs.allSatisfy(Self.isCanonicalBrowserNodeID),
                  let uploadChunkBytes = (object["uploadChunkBytes"] as? NSNumber)?.intValue,
                  (8 * 1024 ... 128 * 1024).contains(uploadChunkBytes),
                  uploadChunkBytes.isMultiple(of: 1024),
                  let nonce = authenticationNonce,
                  let mac = object["mac"] as? String,
                  Self.secureMACEqual(mac, BrowserLinkSignalCipher.authenticationConfirmationMAC(
                    secret: pairing.secret,
                    sessionID: pairing.sessionID,
                    nonce: nonce,
                    folderName: folderName,
                    browserNodeID: browserNodeID,
                    reclaimBrowserNodeIDs: reclaimBrowserNodeIDs,
                    uploadChunkBytes: uploadChunkBytes
                  )),
                  !folderName.isEmpty,
                  folderName.count <= 255 else {
                fail(BrowserLinkClientError.authenticationFailed)
                return
            }
            remoteFolderName = Self.sanitizedFolderName(folderName)
            guard !remoteFolderName.isEmpty else {
                fail(BrowserLinkClientError.authenticationFailed)
                return
            }
            remoteBrowserNodeID = browserNodeID
            self.reclaimBrowserNodeIDs = reclaimBrowserNodeIDs
            self.uploadChunkBytes = uploadChunkBytes
            authenticationNonce = nil
            authenticationTimeoutTask?.cancel()
            connectionTimeoutTask?.cancel()
            setState(.connected)
            browserLinkLog.info("Data channel authentication succeeded protocol=\(BrowserLinkProtocol.version)")
            return
        }
        browserLinkLog.info("Data channel pre-auth message type=\(type, privacy: .public)")
        guard type == "auth_challenge",
              state == .negotiating,
              authenticationGate.acceptChallenge(),
              let nonce = object["nonce"] as? String,
              let nonceBytes = Data(base64URLEncoded: nonce),
              nonceBytes.count == 24,
              nonceBytes.base64URLEncodedString() == nonce else {
            fail(BrowserLinkClientError.authenticationFailed)
            return
        }
        let response: [String: String] = [
            "type": "auth_response",
            "mac": BrowserLinkSignalCipher.authenticationMAC(
                secret: pairing.secret,
                sessionID: pairing.sessionID,
                nonce: nonce
            ),
        ]
        guard let data = try? JSONSerialization.data(withJSONObject: response),
              dataChannel.sendData(RTCDataBuffer(data: data, isBinary: false)) else {
            fail(BrowserLinkClientError.authenticationFailed)
            return
        }
        setState(.authenticating)
        authenticationNonce = nonce
        browserLinkLog.info("Data channel auth response sent")
        authenticationTimeoutTask?.cancel()
        authenticationTimeoutTask = Task { [weak self] in
            try? await Task.sleep(for: .seconds(10))
            guard !Task.isCancelled, let self, self.state != .connected else { return }
            self.fail(BrowserLinkClientError.authenticationFailed)
        }
    }

    private func failDataChannelProtocol() {
        fail(state == .connected
            ? BrowserLinkClientError.invalidServerMessage
            : BrowserLinkClientError.authenticationFailed)
    }

    func fileSystemRequest(
        operation: String,
        arguments: Data,
        priority: FileSystemRequestPriority = .ordinary,
        respectTaskCancellation: Bool = true
    ) async throws -> Data {
        if respectTaskCancellation { try Task.checkCancellation() }
        guard let argumentObject = try JSONSerialization.jsonObject(with: arguments) as? [String: Any] else {
            throw BrowserLinkClientError.connectionClosed
        }
        let pendingRequestLimit = Self.pendingRequestLimit(priority: priority)
        while pendingRequests.count >= pendingRequestLimit {
            try await waitForRequestSlot(
                priority: priority,
                respectTaskCancellation: respectTaskCancellation
            )
        }
        if respectTaskCancellation {
            do {
                try Task.checkCancellation()
            } catch {
                wakeNextRequestSlotWaiter()
                throw error
            }
        }
        guard state == .connected,
              let dataChannel,
              dataChannel.readyState == .open else {
            throw BrowserLinkClientError.connectionClosed
        }
        let requestID = UUID().uuidString.lowercased()
        let diagnosticID = String(requestID.prefix(8))
        browserLinkLog.info("FS request id=\(diagnosticID, privacy: .public) operation=\(operation, privacy: .public)")
        var request = argumentObject
        request["type"] = "fs_request"
        request["id"] = requestID
        request["operation"] = operation
        let data = try JSONSerialization.data(withJSONObject: request)

        if respectTaskCancellation {
            return try await withTaskCancellationHandler {
                try await enqueueFileSystemRequest(
                    requestID: requestID,
                    operation: operation,
                    data: data,
                    dataChannel: dataChannel,
                    rejectIfCancelled: true
                )
            } onCancel: {
                Task { @MainActor [weak self] in
                    guard let self, self.pendingRequests[requestID] != nil else { return }
                    self.finishRequest(requestID, result: .failure(CancellationError()))
                }
            }
        }
        return try await enqueueFileSystemRequest(
            requestID: requestID,
            operation: operation,
            data: data,
            dataChannel: dataChannel,
            rejectIfCancelled: false
        )
    }

    func beginFileSystemUpload(transferID: String, expectedSize: Int64, control: Bool) throws {
        guard state == .connected,
              expectedSize >= 0,
              expectedSize <= BrowserLinkDownloadReceivePolicy.maximumTransferBytes,
              UUID(uuidString: transferID)?.uuidString.lowercased() == transferID,
              outgoingUploads[transferID] == nil else {
            throw BrowserLinkClientError.connectionClosed
        }
        outgoingUploads[transferID] = OutgoingUpload(expectedSize: expectedSize, control: control)
    }

    func sendFileSystemUploadChunk(
        transferID: String,
        offset: Int64,
        payload: Data,
        respectTaskCancellation: Bool
    ) async throws {
        if respectTaskCancellation { try Task.checkCancellation() }
        guard !payload.isEmpty, payload.count <= BrowserLinkFileFrameCodec.maximumPayloadBytes else {
            throw BrowserLinkFileFrameError.invalidFrame
        }
        while true {
            guard state == .connected,
                  let dataChannel,
                  dataChannel.readyState == .open,
                  let upload = outgoingUploads[transferID],
                  upload.sentSize == offset,
                  offset <= upload.expectedSize,
                  Int64(payload.count) <= upload.expectedSize - offset else {
                throw BrowserLinkClientError.connectionClosed
            }
            if let failure = upload.failure { throw failure }
            let outstandingBytes = outgoingUploads.values.reduce(Int64.zero) {
                $0 + max(0, $1.sentSize - $1.acknowledgedSize)
            }
            let classOutstandingBytes = outgoingUploads.values.reduce(Int64.zero) {
                $0 + ($1.control == upload.control ? max(0, $1.sentSize - $1.acknowledgedSize) : 0)
            }
            let payloadBytes = Int64(payload.count)
            let bufferedBytes = Int64(dataChannel.bufferedAmount)
            let bufferedLimit = Self.bufferedUploadLimit(control: upload.control)
            let availableWindow = Self.availableUploadWindowBytes(
                totalOutstandingBytes: outstandingBytes,
                classOutstandingBytes: classOutstandingBytes,
                control: upload.control
            )
            if payloadBytes <= availableWindow,
               bufferedBytes + payloadBytes + Int64(BrowserLinkFileFrameCodec.headerSize) <= Int64(bufferedLimit) {
                let frame = try BrowserLinkFileFrameCodec.encode(
                    kind: .upload,
                    transferID: transferID,
                    offset: offset,
                    payload: payload
                )
                guard dataChannel.sendData(RTCDataBuffer(data: frame, isBinary: true)) else {
                    throw BrowserLinkClientError.connectionClosed
                }
                outgoingUploads[transferID]?.sentSize += payloadBytes
                return
            }
            try await waitForUploadFlowChange(respectTaskCancellation: respectTaskCancellation)
        }
    }

    func endFileSystemUpload(transferID: String) {
        outgoingUploads.removeValue(forKey: transferID)
        wakeUploadFlowWaiters()
    }

    nonisolated static func availableUploadWindowBytes(
        totalOutstandingBytes: Int64,
        classOutstandingBytes: Int64,
        control: Bool
    ) -> Int64 {
        let classLimit = control
            ? maximumControlUnacknowledgedUploadBytes
            : maximumDataUnacknowledgedUploadBytes
        return min(
            Int64(maximumUnacknowledgedUploadBytes) - totalOutstandingBytes,
            Int64(classLimit) - classOutstandingBytes
        )
    }

    nonisolated static func bufferedUploadLimit(control: Bool) -> Int {
        control ? maximumBufferedUploadBytes : maximumDataBufferedUploadBytes
    }

    func fileSystemUploadFailure(transferID: String) -> Error? {
        outgoingUploads[transferID]?.failure
    }

    func beginFileSystemDownload(
        transferID: String,
        expectedSize: Int64
    ) throws -> AsyncThrowingStream<Data, Error> {
        guard state == .connected,
              expectedSize >= 0,
              expectedSize <= BrowserLinkDownloadReceivePolicy.maximumTransferBytes,
              UUID(uuidString: transferID)?.uuidString.lowercased() == transferID,
              incomingDownloads[transferID] == nil,
              !ignoredIncomingDownloadIDs.contains(transferID) else {
            throw BrowserLinkClientError.connectionClosed
        }
        guard incomingDownloads.count < Self.maximumIncomingDownloads else {
            throw RemoteStorageClientError.unavailable
        }
        var continuation: AsyncThrowingStream<Data, Error>.Continuation!
        let stream = AsyncThrowingStream<Data, Error>(
            bufferingPolicy: .bufferingOldest(Self.maximumBufferedDownloadFrames)
        ) { continuation = $0 }
        incomingDownloads[transferID] = IncomingDownload(
            expectedSize: expectedSize,
            continuation: continuation,
            idleTimeoutTask: nil
        )
        return stream
    }

    func startFileSystemDownload(transferID: String) throws {
        guard incomingDownloads[transferID] != nil else {
            throw BrowserLinkClientError.connectionClosed
        }
        armDownloadIdleTimeout(transferID: transferID)
    }

    func acknowledgeFileSystemDownload(transferID: String, receivedSize: Int64) throws {
        guard state == .connected,
              let dataChannel,
              dataChannel.readyState == .open,
              var download = incomingDownloads[transferID],
              receivedSize >= download.acknowledgedSize,
              receivedSize <= download.receivedSize else {
            throw BrowserLinkClientError.connectionClosed
        }
        let acknowledgement: [String: Any] = [
            "type": "fs_download_ack",
            "transferID": transferID,
            "receivedSize": receivedSize,
        ]
        let data = try JSONSerialization.data(withJSONObject: acknowledgement)
        guard dataChannel.sendData(RTCDataBuffer(data: data, isBinary: false)) else {
            throw BrowserLinkClientError.connectionClosed
        }
        download.acknowledgedSize = receivedSize
        incomingDownloads[transferID] = download
    }

    func endFileSystemDownload(
        transferID: String,
        error: Error? = nil,
        tolerateLateFrames: Bool = false
    ) {
        guard let download = incomingDownloads.removeValue(forKey: transferID) else {
            if error == nil { ignoredIncomingDownloadIDs.remove(transferID) }
            return
        }
        download.idleTimeoutTask?.cancel()
        if tolerateLateFrames { ignoredIncomingDownloadIDs.insert(transferID) }
        if let error { download.continuation.finish(throwing: error) }
        else { download.continuation.finish() }
    }

    func abandonFileSystemDownload(transferID: String, error: Error) {
        if incomingDownloads[transferID] != nil {
            endFileSystemDownload(
                transferID: transferID,
                error: error,
                tolerateLateFrames: true
            )
        }
        guard ignoredIncomingDownloadIDs.contains(transferID),
              pendingDownloadAbortIDs.insert(transferID).inserted else { return }
        let cleanupTask = Task { @MainActor [weak self] in
            guard let self else { return }
            var abortConfirmed = false
            if let arguments = try? JSONSerialization.data(
                withJSONObject: ["transferID": transferID]
            ) {
                do {
                    _ = try await self.fileSystemRequest(
                        operation: "download_abort",
                        arguments: arguments,
                        priority: .cleanup,
                        respectTaskCancellation: false
                    )
                    abortConfirmed = true
                } catch {}
            }
            if !abortConfirmed, !self.stopped {
                try? await Task.sleep(for: Self.abandonedDownloadRetention)
            }
            self.pendingDownloadAbortIDs.remove(transferID)
            self.stopIgnoringFileSystemDownload(transferID: transferID)
            self.downloadAbortCleanupTasks.removeValue(forKey: transferID)
        }
        downloadAbortCleanupTasks[transferID] = cleanupTask
    }

    func cancelFileSystemDownloadIdleTimeout(transferID: String) {
        incomingDownloads[transferID]?.idleTimeoutTask?.cancel()
        incomingDownloads[transferID]?.idleTimeoutTask = nil
    }

    func stopIgnoringFileSystemDownload(transferID: String) {
        ignoredIncomingDownloadIDs.remove(transferID)
    }

    private func enqueueFileSystemRequest(
        requestID: String,
        operation: String,
        data: Data,
        dataChannel: RTCDataChannel,
        rejectIfCancelled: Bool
    ) async throws -> Data {
        try await withCheckedThrowingContinuation { continuation in
            pendingRequests[requestID] = PendingRequest(
                continuation: continuation,
                timeoutTask: makeRequestTimeoutTask(requestID: requestID, operation: operation)
            )
            if rejectIfCancelled, Task.isCancelled {
                finishRequest(requestID, result: .failure(CancellationError()))
                return
            }
            guard dataChannel.sendData(RTCDataBuffer(data: data, isBinary: false)) else {
                finishRequest(requestID, result: .failure(BrowserLinkClientError.connectionClosed))
                return
            }
        }
    }

    private func handleFileSystemResponse(_ object: [String: Any], type: String) {
        if type == "fs_upload_ack" {
            handleUploadAcknowledgement(object)
            return
        }
        if type == "fs_upload_error" {
            handleUploadError(object)
            return
        }
        if type == "fs_download_error" {
            handleDownloadError(object)
            return
        }
        if type == "fs_response_part" {
            handleFileSystemResponsePart(object)
            return
        }
        guard type == "fs_response",
              let requestID = object["id"] as? String,
              let succeeded = object["ok"] as? Bool else {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        guard pendingRequests[requestID] != nil else {
            browserLinkLog.info("Ignored late FS response id=\(String(requestID.prefix(8)), privacy: .public)")
            return
        }
        if succeeded {
            browserLinkLog.info("FS response id=\(String(requestID.prefix(8)), privacy: .public) ok=true")
            let result = object["result"] ?? NSNull()
            guard let data = try? JSONSerialization.data(withJSONObject: result, options: [.fragmentsAllowed]) else {
                finishRequest(requestID, result: .failure(BrowserLinkClientError.invalidServerMessage))
                return
            }
            finishRequest(requestID, result: .success(data))
        } else {
            let code = object["error"] as? String ?? "remote_error"
            browserLinkLog.error("FS response id=\(String(requestID.prefix(8)), privacy: .public) ok=false code=\(code, privacy: .public)")
            finishRequest(requestID, result: .failure(BrowserLinkFileSystemError.remote(code)))
        }
    }

    private func handleUploadAcknowledgement(_ object: [String: Any]) {
        guard let transferID = object["transferID"] as? String,
              let value = object["receivedSize"] as? NSNumber,
              value.doubleValue.rounded(.towardZero) == value.doubleValue else {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        guard var upload = outgoingUploads[transferID] else {
            browserLinkLog.info("Ignored late upload acknowledgement")
            return
        }
        let receivedSize = value.int64Value
        let acceptedSize: Int64?
        do {
            acceptedSize = try Self.acceptedAcknowledgement(
                current: upload.acknowledgedSize,
                sent: upload.sentSize,
                received: receivedSize
            )
        } catch {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        guard let acceptedSize else {
            browserLinkLog.info("Ignored stale upload acknowledgement")
            return
        }
        upload.acknowledgedSize = acceptedSize
        outgoingUploads[transferID] = upload
        wakeUploadFlowWaiters()
    }

    private func handleUploadError(_ object: [String: Any]) {
        guard let transferID = object["transferID"] as? String,
              let code = object["error"] as? String,
              !code.isEmpty,
              code.count <= 64 else {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        guard outgoingUploads[transferID] != nil else {
            browserLinkLog.info("Ignored late upload error")
            return
        }
        outgoingUploads[transferID]?.failure = BrowserLinkFileSystemError.remote(code)
        wakeUploadFlowWaiters()
    }

    private func handleDownloadFrame(_ data: Data) {
        let frame: BrowserLinkFileFrame
        do {
            frame = try BrowserLinkFileFrameCodec.decode(data, expectedKind: .download)
        } catch {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        if ignoredIncomingDownloadIDs.contains(frame.transferID) { return }
        guard var download = incomingDownloads[frame.transferID] else {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        do {
            let totalUnacknowledgedBytes = incomingDownloads.values.reduce(Int64.zero) {
                $0 + max(0, $1.receivedSize - $1.acknowledgedSize)
            }
            download.receivedSize = try BrowserLinkDownloadReceivePolicy.nextReceivedSize(
                expectedSize: download.expectedSize,
                receivedSize: download.receivedSize,
                acknowledgedSize: download.acknowledgedSize,
                totalUnacknowledgedBytes: totalUnacknowledgedBytes,
                frameOffset: frame.offset,
                payloadSize: frame.payload.count
            )
        } catch {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        incomingDownloads[frame.transferID] = download
        armDownloadIdleTimeout(transferID: frame.transferID)
        switch Self.downloadYieldDisposition(download.continuation.yield(frame.payload)) {
        case .accepted:
            break
        case .abandoned:
            abandonFileSystemDownload(
                transferID: frame.transferID,
                error: CancellationError()
            )
        case .protocolFailure:
            fail(BrowserLinkClientError.invalidServerMessage)
        }
    }

    nonisolated static func downloadYieldDisposition(
        _ result: AsyncThrowingStream<Data, Error>.Continuation.YieldResult
    ) -> BrowserLinkDownloadYieldDisposition {
        switch result {
        case .enqueued:
            .accepted
        case .terminated:
            .abandoned
        case .dropped:
            .protocolFailure
        @unknown default:
            .protocolFailure
        }
    }

    private func handleDownloadError(_ object: [String: Any]) {
        guard let transferID = object["transferID"] as? String,
              let code = object["error"] as? String,
              !code.isEmpty,
              code.count <= 64 else {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        if ignoredIncomingDownloadIDs.remove(transferID) != nil {
            pendingDownloadAbortIDs.remove(transferID)
            downloadAbortCleanupTasks.removeValue(forKey: transferID)?.cancel()
            return
        }
        guard incomingDownloads[transferID] != nil else {
            browserLinkLog.info("Ignored late download error")
            return
        }
        endFileSystemDownload(
            transferID: transferID,
            error: BrowserLinkFileSystemError.remote(code)
        )
    }

    private func finishIncomingDownloads(error: Error, tolerateLateFrames: Bool = false) {
        let downloads = incomingDownloads
        incomingDownloads.removeAll()
        if tolerateLateFrames {
            ignoredIncomingDownloadIDs.formUnion(downloads.keys)
        }
        downloads.values.forEach {
            $0.idleTimeoutTask?.cancel()
            $0.continuation.finish(throwing: error)
        }
        if tolerateLateFrames {
            for transferID in downloads.keys {
                abandonFileSystemDownload(transferID: transferID, error: error)
            }
        }
    }

    private func armDownloadIdleTimeout(transferID: String) {
        incomingDownloads[transferID]?.idleTimeoutTask?.cancel()
        incomingDownloads[transferID]?.idleTimeoutTask = Task { [weak self] in
            do {
                try await Task.sleep(for: .seconds(65))
            } catch {
                return
            }
            guard !Task.isCancelled, let self, self.incomingDownloads[transferID] != nil else { return }
            self.abandonFileSystemDownload(
                transferID: transferID,
                error: BrowserLinkFileSystemError.remote("transfer_timeout")
            )
        }
    }

    private func waitForUploadFlowChange(respectTaskCancellation: Bool) async throws {
        let waiterID = UUID()
        let timeoutTask = Task { [weak self] in
            do {
                try await Task.sleep(for: Self.uploadFlowTimeout)
            } catch {
                return
            }
            guard !Task.isCancelled else { return }
            self?.finishUploadFlowWaiter(
                waiterID,
                result: .failure(BrowserLinkFileSystemError.remote("transfer_timeout"))
            )
        }
        defer { timeoutTask.cancel() }
        let operation: () async throws -> Void = {
            try await withCheckedThrowingContinuation { continuation in
                self.uploadFlowWaiters[waiterID] = continuation
                if self.stopped {
                    self.finishUploadFlowWaiter(
                        waiterID,
                        result: .failure(BrowserLinkClientError.connectionClosed)
                    )
                }
            }
        }
        if respectTaskCancellation {
            try await withTaskCancellationHandler(operation: operation) {
                Task { @MainActor [weak self] in
                    self?.finishUploadFlowWaiter(waiterID, result: .failure(CancellationError()))
                }
            }
        } else {
            try await operation()
        }
    }

    private func finishUploadFlowWaiter(_ id: UUID, result: Result<Void, Error>) {
        guard let continuation = uploadFlowWaiters.removeValue(forKey: id) else { return }
        continuation.resume(with: result)
    }

    private func wakeUploadFlowWaiters(error: Error? = nil) {
        let waiters = uploadFlowWaiters
        uploadFlowWaiters.removeAll()
        for continuation in waiters.values {
            if let error { continuation.resume(throwing: error) }
            else { continuation.resume() }
        }
    }

    private func handleFileSystemResponsePart(_ object: [String: Any]) {
        guard let requestID = object["id"] as? String,
              let index = (object["index"] as? NSNumber)?.intValue,
              let total = (object["total"] as? NSNumber)?.intValue,
              let encoded = object["data"] as? String,
              let part = Data(base64Encoded: encoded) else {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        guard var pending = pendingRequests[requestID] else {
            browserLinkLog.info("Ignored late FS response part id=\(String(requestID.prefix(8)), privacy: .public)")
            return
        }
        let pendingBytes = pendingRequests.values.reduce(0) { $0 + $1.responseAssembler.assembledByteCount }
        guard pendingBytes + part.count <= Self.maximumPendingResponseBytes else {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        let result: Data?
        do {
            result = try pending.responseAssembler.append(index: index, total: total, part: part)
        } catch {
            fail(BrowserLinkClientError.invalidServerMessage)
            return
        }
        pending.timeoutTask.cancel()
        pending.timeoutTask = makeRequestTimeoutTask(requestID: requestID, operation: "response_parts")
        pendingRequests[requestID] = pending
        guard let result else { return }
        browserLinkLog.info("FS response id=\(String(requestID.prefix(8)), privacy: .public) ok=true parts=\(total)")
        finishRequest(requestID, result: .success(result))
    }

    nonisolated static func fileSystemRequestTimeoutSeconds(operation: String) -> Int {
        300
    }

    private func makeRequestTimeoutTask(requestID: String, operation: String) -> Task<Void, Never> {
        let timeout = Duration.seconds(Self.fileSystemRequestTimeoutSeconds(operation: operation))
        return Task { [weak self] in
            try? await Task.sleep(for: timeout)
            guard !Task.isCancelled, let self, self.pendingRequests[requestID] != nil else { return }
            if Self.requestTimeoutClosesSession(operation: operation) {
                self.fail(BrowserLinkClientError.requestFailed)
            } else {
                self.finishRequest(requestID, result: .failure(RemoteStorageClientError.unavailable))
            }
        }
    }

    nonisolated static func requestTimeoutClosesSession(operation: String) -> Bool {
        operation != "list" && operation != "metadata" && operation != "response_parts"
    }

    nonisolated static func pendingRequestLimit(priority: FileSystemRequestPriority) -> Int {
        switch priority {
        case .ordinary: maximumOrdinaryPendingRequests
        case .control: maximumControlPendingRequests
        case .cleanup: maximumPendingRequests
        }
    }

    nonisolated static func requestSlotWaiterLimit(priority: FileSystemRequestPriority) -> Int {
        switch priority {
        case .ordinary: maximumOrdinaryRequestSlotWaiters
        case .control: maximumControlRequestSlotWaiters
        case .cleanup: maximumRequestSlotWaiters
        }
    }

    nonisolated static func nextRequestSlotWaiterID(
        order: [UUID],
        priorities: [UUID: FileSystemRequestPriority],
        pendingRequestCount: Int
    ) -> UUID? {
        for priority in [FileSystemRequestPriority.cleanup, .control, .ordinary]
        where pendingRequestCount < pendingRequestLimit(priority: priority) {
            if let id = order.first(where: { priorities[$0] == priority }) { return id }
        }
        return nil
    }

    private func finishRequest(_ requestID: String, result: Result<Data, Error>) {
        guard let pending = pendingRequests.removeValue(forKey: requestID) else { return }
        pending.timeoutTask.cancel()
        pending.continuation.resume(with: result)
        wakeNextRequestSlotWaiter()
    }

    private func waitForRequestSlot(
        priority: FileSystemRequestPriority,
        respectTaskCancellation: Bool
    ) async throws {
        guard requestSlotWaiters.count < Self.requestSlotWaiterLimit(priority: priority) else {
            throw RemoteStorageClientError.unavailable
        }
        let waiterID = UUID()
        let timeoutTask = Task { [weak self] in
            do {
                try await Task.sleep(for: .seconds(30))
            } catch {
                return
            }
            guard !Task.isCancelled else { return }
            self?.finishRequestSlotWaiter(waiterID, result: .failure(RemoteStorageClientError.unavailable))
        }
        defer { timeoutTask.cancel() }
        let operation: () async throws -> Void = {
            try await withCheckedThrowingContinuation { continuation in
                self.requestSlotWaiters[waiterID] = RequestSlotWaiter(
                    priority: priority,
                    continuation: continuation
                )
                self.requestSlotWaiterOrder.append(waiterID)
                if self.stopped {
                    self.finishRequestSlotWaiter(
                        waiterID,
                        result: .failure(BrowserLinkClientError.connectionClosed)
                    )
                }
            }
        }
        if respectTaskCancellation {
            try await withTaskCancellationHandler(operation: operation) {
                Task { @MainActor [weak self] in
                    self?.finishRequestSlotWaiter(waiterID, result: .failure(CancellationError()))
                }
            }
        } else {
            try await operation()
        }
    }

    private func finishRequestSlotWaiter(_ id: UUID, result: Result<Void, Error>) {
        guard let waiter = requestSlotWaiters.removeValue(forKey: id) else { return }
        requestSlotWaiterOrder.removeAll { $0 == id }
        waiter.continuation.resume(with: result)
        if !stopped { wakeNextRequestSlotWaiter() }
    }

    private func wakeNextRequestSlotWaiter() {
        let priorities = requestSlotWaiters.mapValues(\.priority)
        guard let id = Self.nextRequestSlotWaiterID(
            order: requestSlotWaiterOrder,
            priorities: priorities,
            pendingRequestCount: pendingRequests.count
        ), let index = requestSlotWaiterOrder.firstIndex(of: id),
           let waiter = requestSlotWaiters.removeValue(forKey: id) else { return }
        requestSlotWaiterOrder.remove(at: index)
        waiter.continuation.resume()
    }

    private func wakeRequestSlotWaiters(error: Error) {
        let waiters = requestSlotWaiters
        requestSlotWaiters.removeAll()
        requestSlotWaiterOrder.removeAll()
        for waiter in waiters.values {
            waiter.continuation.resume(throwing: error)
        }
    }

    private func setState(_ newState: BrowserLinkClientState) {
        browserLinkLog.info("Client state \(Self.diagnosticState(self.state), privacy: .public) -> \(Self.diagnosticState(newState), privacy: .public)")
        state = newState
        onStateChange?(newState)
    }

    private func fail(_ error: Error) {
        guard !stopped else { return }
        browserLinkLog.error("Client failed state=\(Self.diagnosticState(self.state), privacy: .public) type=\(String(reflecting: type(of: error)), privacy: .public) message=\(error.localizedDescription, privacy: .public)")
        setState(.failed(error.localizedDescription))
        dispose(sendCancel: false, preserveState: true)
        onTerminalFailure?(error)
    }

    private func dispose(sendCancel: Bool, preserveState: Bool = false) {
        guard !stopped else { return }
        browserLinkLog.info("Client dispose sendCancel=\(sendCancel) pendingRequests=\(self.pendingRequests.count)")
        stopped = true
        if sendCancel, let socket = webSocket {
            let data = try? JSONSerialization.data(withJSONObject: ["kind": "cancel"])
            if let data { socket.send(.string(String(decoding: data, as: UTF8.self))) { _ in } }
        }
        receiveTask?.cancel()
        authenticationTimeoutTask?.cancel()
        connectionTimeoutTask?.cancel()
        disconnectionTimeoutTask?.cancel()
        downloadAbortCleanupTasks.values.forEach { $0.cancel() }
        downloadAbortCleanupTasks.removeAll()
        pendingDownloadAbortIDs.removeAll()
        pathMonitor.cancel()
        webSocket?.cancel(with: .normalClosure, reason: nil)
        dataChannel?.close()
        peerConnection?.close()
        let pending = pendingRequests
        pendingRequests.removeAll()
        for request in pending.values {
            request.timeoutTask.cancel()
            request.continuation.resume(throwing: BrowserLinkClientError.connectionClosed)
        }
        outgoingUploads.removeAll()
        finishIncomingDownloads(error: BrowserLinkClientError.connectionClosed)
        ignoredIncomingDownloadIDs.removeAll()
        incomingMessageBuffer.removeAll()
        ingressSequencer.reset()
        wakeUploadFlowWaiters(error: BrowserLinkClientError.connectionClosed)
        wakeRequestSlotWaiters(error: BrowserLinkClientError.connectionClosed)
        webSocket = nil
        dataChannel = nil
        peerConnection = nil
        if !preserveState {
            onStateChange = nil
            onTerminalFailure = nil
        }
    }

    private static func diagnosticState(_ state: BrowserLinkClientState) -> String {
        switch state {
        case .connecting: "connecting"
        case .waitingForDesktop: "waitingForDesktop"
        case .negotiating: "negotiating"
        case .authenticating: "authenticating"
        case .connected: "connected"
        case .failed: "failed"
        }
    }

    nonisolated static func acceptedAcknowledgement(
        current: Int64,
        sent: Int64,
        received: Int64
    ) throws -> Int64? {
        guard received <= sent else { throw BrowserLinkClientError.invalidServerMessage }
        return received < current ? nil : received
    }

    private static func sanitizedFolderName(_ value: String) -> String {
        let bidiControls: Set<UInt32> = [0x202A, 0x202B, 0x202C, 0x202D, 0x202E, 0x2066, 0x2067, 0x2068, 0x2069]
        let scalars = value.unicodeScalars.filter {
            !CharacterSet.controlCharacters.contains($0) && !bidiControls.contains($0.value)
        }
        return String(String.UnicodeScalarView(scalars)).trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func secureMACEqual(_ lhs: String, _ rhs: String) -> Bool {
        guard let left = Data(base64URLEncoded: lhs),
              let right = Data(base64URLEncoded: rhs),
              left.count == right.count else { return false }
        return zip(left, right).reduce(UInt8.zero) { $0 | ($1.0 ^ $1.1) } == 0
    }

    private static func isCanonicalBrowserNodeID(_ value: String) -> Bool {
        guard value.count == 43,
              let data = Data(base64URLEncoded: value),
              data.count == 32 else { return false }
        return data.base64URLEncodedString() == value
    }

    private func setRemoteDescription(_ description: RTCSessionDescription, on connection: RTCPeerConnection) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            connection.setRemoteDescription(description) { error in
                if let error { continuation.resume(throwing: error) }
                else { continuation.resume() }
            }
        }
    }

    private func createAnswer(
        on connection: RTCPeerConnection,
        constraints: RTCMediaConstraints
    ) async throws -> RTCSessionDescription {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<RTCSessionDescription, Error>) in
            connection.answer(for: constraints) { description, error in
                if let error { continuation.resume(throwing: error) }
                else if let description { continuation.resume(returning: description) }
                else { continuation.resume(throwing: BrowserLinkClientError.webRTCUnavailable) }
            }
        }
    }

    private func setLocalDescription(_ description: RTCSessionDescription, on connection: RTCPeerConnection) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            connection.setLocalDescription(description) { error in
                if let error { continuation.resume(throwing: error) }
                else { continuation.resume() }
            }
        }
    }

    private func add(_ candidate: RTCIceCandidate, to connection: RTCPeerConnection) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            connection.add(candidate) { error in
                if let error { continuation.resume(throwing: error) }
                else { continuation.resume() }
            }
        }
    }
}

extension BrowserLinkClient: RTCPeerConnectionDelegate {
    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didChange stateChanged: RTCSignalingState) {
        browserLinkLog.info("Peer signaling state=\(String(describing: stateChanged), privacy: .public)")
    }
    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didAdd stream: RTCMediaStream) {}
    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didRemove stream: RTCMediaStream) {}
    nonisolated func peerConnectionShouldNegotiate(_ peerConnection: RTCPeerConnection) {}
    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didChange newState: RTCIceConnectionState) {
        browserLinkLog.info("ICE connection state=\(String(describing: newState), privacy: .public)")
    }
    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didChange newState: RTCIceGatheringState) {
        browserLinkLog.info("ICE gathering state=\(String(describing: newState), privacy: .public)")
    }
    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didRemove candidates: [RTCIceCandidate]) {}

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didGenerate candidate: RTCIceCandidate) {
        let label = BrowserLinkICEPolicy.diagnosticLabel(for: candidate.sdp)
        browserLinkLog.info("ICE candidate generated \(label, privacy: .public)")
        Task { @MainActor [weak self] in
            guard let self, !self.stopped else { return }
            if self.localDescriptionSent {
                do { try await self.send(candidate) }
                catch { self.fail(error) }
            } else {
                self.pendingLocalCandidates.append(candidate)
            }
        }
    }

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didOpen dataChannel: RTCDataChannel) {
        browserLinkLog.info("Peer opened data channel labelAccepted=\(dataChannel.label == "watermelon-link-v1") state=\(String(describing: dataChannel.readyState), privacy: .public)")
        Task { @MainActor [weak self] in
            guard let self,
                  !self.stopped,
                  self.peerConnection === peerConnection,
                  dataChannel.label == "watermelon-link-v1",
                  self.dataChannel == nil else {
                dataChannel.close()
                return
            }
            self.dataChannel = dataChannel
            dataChannel.delegate = self
            let ready: [String: Any] = [
                "type": "auth_ready",
                "protocolVersion": BrowserLinkProtocol.version,
            ]
            guard let data = try? JSONSerialization.data(withJSONObject: ready),
                  dataChannel.sendData(RTCDataBuffer(data: data, isBinary: false)) else {
                self.fail(BrowserLinkClientError.authenticationFailed)
                return
            }
            browserLinkLog.info("Data channel authentication ready sent")
        }
    }

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didChange newState: RTCPeerConnectionState) {
        browserLinkLog.info("Peer connection state=\(String(describing: newState), privacy: .public)")
        Task { @MainActor [weak self] in
            guard let self, !self.stopped else { return }
            switch newState {
            case .connected:
                self.disconnectionTimeoutTask?.cancel()
                self.disconnectionTimeoutTask = nil
            case .disconnected:
                self.disconnectionTimeoutTask?.cancel()
                self.disconnectionTimeoutTask = Task { [weak self, weak peerConnection] in
                    try? await Task.sleep(for: .seconds(5))
                    guard !Task.isCancelled,
                          let self,
                          let peerConnection,
                          peerConnection.connectionState == .disconnected else { return }
                    self.fail(BrowserLinkClientError.connectionClosed)
                }
            case .failed, .closed:
                self.fail(BrowserLinkClientError.connectionClosed)
            default:
                break
            }
        }
    }
}

extension BrowserLinkClient: RTCDataChannelDelegate {
    nonisolated func dataChannelDidChangeState(_ dataChannel: RTCDataChannel) {
        browserLinkLog.info("Data channel state=\(String(describing: dataChannel.readyState), privacy: .public)")
        guard dataChannel.readyState == .closed else { return }
        Task { @MainActor [weak self] in
            guard let self, self.dataChannel === dataChannel, !self.stopped else { return }
            self.fail(BrowserLinkClientError.connectionClosed)
        }
    }

    nonisolated func dataChannel(_ dataChannel: RTCDataChannel, didReceiveMessageWith buffer: RTCDataBuffer) {
        guard let sequence = ingressSequencer.reserveSequence(
            messageBytes: buffer.data.count,
            maximumMessages: Self.maximumPendingIncomingMessages,
            maximumBytes: Self.maximumPendingIncomingMessageBytes
        ) else {
            guard ingressSequencer.claimOverflowNotification() else { return }
            Task { @MainActor [weak self] in self?.fail(BrowserLinkClientError.invalidServerMessage) }
            return
        }
        let message = BrowserLinkIncomingDataMessage(
            channelIdentity: ObjectIdentifier(dataChannel),
            data: buffer.data,
            isBinary: buffer.isBinary
        )
        Task { @MainActor [weak self] in
            self?.enqueueDataChannelMessage(sequence: sequence, message: message)
        }
    }

    nonisolated func dataChannel(_ dataChannel: RTCDataChannel, didChangeBufferedAmount _: UInt64) {
        Task { @MainActor [weak self] in
            guard let self, self.dataChannel === dataChannel else { return }
            self.wakeUploadFlowWaiters()
        }
    }
}
