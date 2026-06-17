import Foundation

// Pre-resolves mDNS `.local` hostnames to an IPv4 literal so socket clients skip the system resolver's ~5s
// dual-stack (A+AAAA) wait: for `.local`, AAAA never answers and the resolver blocks until it times out.
// Only `.local` is touched — real DNS resolves dual-stack / IPv6-only / NAT64 correctly itself and must not
// be forced onto IPv4. Any failure/timeout returns nil so the caller falls back to the original hostname.
enum HostnameResolver {
    static func resolvedIPv4(_ host: String, timeoutSeconds: Double = 2.0) async -> String? {
        let trimmed = host.trimmingCharacters(in: .whitespacesAndNewlines)
        guard isMDNSLocalName(trimmed) else { return nil }

        return await withCheckedContinuation { continuation in
            let gate = ResumeGate(continuation)
            DispatchQueue.global(qos: .userInitiated).async {
                gate.resume(with: synchronousResolveIPv4(trimmed))
            }
            DispatchQueue.global().asyncAfter(deadline: .now() + timeoutSeconds) {
                gate.resume(with: nil)
            }
        }
    }

    private static func isMDNSLocalName(_ host: String) -> Bool {
        let lower = host.lowercased()
        return lower.hasSuffix(".local") || lower.hasSuffix(".local.")
    }

    // Resumes exactly once — whichever of resolve / timeout fires first wins; the loser is a no-op. The losing
    // getaddrinfo keeps running on its GCD thread until it returns, but its result is discarded.
    private final class ResumeGate: @unchecked Sendable {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<String?, Never>?
        init(_ continuation: CheckedContinuation<String?, Never>) { self.continuation = continuation }
        func resume(with value: String?) {
            lock.lock()
            let pending = continuation
            continuation = nil
            lock.unlock()
            pending?.resume(returning: value)
        }
    }

    private static func synchronousResolveIPv4(_ host: String) -> String? {
        var hints = addrinfo(
            ai_flags: 0,
            ai_family: AF_INET,
            ai_socktype: SOCK_STREAM,
            ai_protocol: IPPROTO_TCP,
            ai_addrlen: 0,
            ai_canonname: nil,
            ai_addr: nil,
            ai_next: nil
        )
        var result: UnsafeMutablePointer<addrinfo>?
        guard getaddrinfo(host, nil, &hints, &result) == 0, let head = result else { return nil }
        defer { freeaddrinfo(result) }

        var node: UnsafeMutablePointer<addrinfo>? = head
        while let current = node {
            if current.pointee.ai_family == AF_INET, let addr = current.pointee.ai_addr {
                let ip = addr.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { sin -> String? in
                    var sinAddr = sin.pointee.sin_addr
                    var buffer = [CChar](repeating: 0, count: Int(INET_ADDRSTRLEN))
                    guard inet_ntop(AF_INET, &sinAddr, &buffer, socklen_t(INET_ADDRSTRLEN)) != nil else { return nil }
                    return String(cString: buffer)
                }
                if let ip { return ip }
            }
            node = current.pointee.ai_next
        }
        return nil
    }
}
