protocol RemoteLeasedNamespaceClient: AnyObject, Sendable {
    var allowsUnattendedLeaseConfidence: Bool { get }
    func beginLeasedNamespaceSession() async
    func endLeasedNamespaceSession() async
}

extension RemoteLeasedNamespaceClient {
    var allowsUnattendedLeaseConfidence: Bool { false }
}
