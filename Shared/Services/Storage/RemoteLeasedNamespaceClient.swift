protocol RemoteLeasedNamespaceClient: AnyObject, Sendable {
    func beginLeasedNamespaceSession() async
    func endLeasedNamespaceSession() async
}
