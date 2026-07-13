import Foundation
import Network

enum BrowserLinkICEPolicy {
    static func allows(candidateSDP: String) -> Bool {
        guard candidateSDP.unicodeScalars.allSatisfy({ (0x20...0x7E).contains($0.value) }) else {
            return false
        }
        let fields = candidateSDP.split(separator: " ", omittingEmptySubsequences: true).map(String.init)
        guard fields.count >= 8,
              isValidFoundation(fields[0]),
              fields[1] == "1" || fields[1] == "2",
              fields[2].lowercased() == "udp" || fields[2].lowercased() == "tcp",
              isCanonicalUnsigned(fields[3], maximum: UInt64(UInt32.max), permitsZero: true),
              isCanonicalUnsigned(fields[5], maximum: UInt64(UInt16.max), permitsZero: false),
              fields[6].lowercased() == "typ",
              fields[7].lowercased() == "host" else {
            return false
        }
        return isLocalAddress(fields[4])
    }

    static func filteringCandidates(in sdp: String) -> String {
        guard hasValidSDPCharacters(sdp) else { return "" }
        return logicalLines(in: sdp)
            .compactMap { line -> String? in
                let normalized = line.trimmingCharacters(in: .whitespacesAndNewlines)
                switch candidateLine(normalized) {
                case .none:
                    return line
                case .malformed:
                    return nil
                case .candidate(let value):
                    return allows(candidateSDP: value) ? line : nil
                }
            }
            .joined(separator: "\r\n")
    }

    static func statistics(in sdp: String) -> (total: Int, allowed: Int) {
        guard hasValidSDPCharacters(sdp) else { return (0, 0) }
        let candidates = logicalLines(in: sdp).compactMap { line -> CandidateLine? in
            let normalized = line.trimmingCharacters(in: .whitespacesAndNewlines)
            let candidate = candidateLine(normalized)
            if case .none = candidate { return nil }
            return candidate
        }
        let allowed = candidates.filter {
            if case .candidate(let value) = $0 { return allows(candidateSDP: value) }
            return false
        }.count
        return (candidates.count, allowed)
    }

    static func diagnosticLabel(for candidateSDP: String) -> String {
        let fields = candidateSDP.split(whereSeparator: \.isWhitespace).map(String.init)
        guard fields.count >= 8 else { return "malformed" }
        let candidateType = fields[7].lowercased()
        let address = fields[4].lowercased()
        let addressKind: String
        if address.hasSuffix(".local") {
            addressKind = "mdns"
        } else if IPv4Address(address) != nil {
            addressKind = isLocalAddress(address) ? "private-ipv4" : "public-ipv4"
        } else if IPv6Address(address.split(separator: "%", maxSplits: 1).first.map(String.init) ?? address) != nil {
            addressKind = isLocalAddress(address) ? "local-ipv6" : "public-ipv6"
        } else {
            addressKind = "hostname"
        }
        return "\(candidateType)/\(addressKind)/\(allows(candidateSDP: candidateSDP) ? "allowed" : "rejected")"
    }

    private static func isLocalAddress(_ rawAddress: String) -> Bool {
        let address = rawAddress.lowercased()
        if isValidMDNSName(address) { return true }

        if isCanonicalIPv4(address), let ipv4 = IPv4Address(address) {
            let bytes = [UInt8](ipv4.rawValue)
            return bytes[0] == 10 ||
                (bytes[0] == 172 && (16...31).contains(bytes[1])) ||
                (bytes[0] == 192 && bytes[1] == 168) ||
                (bytes[0] == 169 && bytes[1] == 254)
        }

        if !address.contains("%"), let ipv6 = IPv6Address(address) {
            let bytes = [UInt8](ipv6.rawValue)
            return (bytes[0] & 0xFE) == 0xFC ||
                (bytes[0] == 0xFE && (bytes[1] & 0xC0) == 0x80)
        }
        return false
    }

    private enum CandidateLine {
        case none
        case malformed
        case candidate(String)
    }

    private static func logicalLines(in sdp: String) -> [String] {
        sdp.replacingOccurrences(of: "\r\n", with: "\n")
            .replacingOccurrences(of: "\r", with: "\n")
            .components(separatedBy: "\n")
    }

    private static func candidateLine(_ line: String) -> CandidateLine {
        if line.lowercased().hasPrefix("a=candidate:") {
            return .candidate(String(line.dropFirst(2)))
        }
        if line.range(
            of: #"^a\s*=\s*candidate\s*:"#,
            options: [.regularExpression, .caseInsensitive]
        ) != nil {
            return .malformed
        }
        return .none
    }

    private static func isValidFoundation(_ value: String) -> Bool {
        guard value.lowercased().hasPrefix("candidate:") else { return false }
        let foundation = value.dropFirst("candidate:".count)
        return (1...32).contains(foundation.count) && foundation.allSatisfy {
            $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "+" || $0 == "/")
        }
    }

    private static func isCanonicalUnsigned(_ value: String, maximum: UInt64, permitsZero: Bool) -> Bool {
        guard !value.isEmpty,
              value.allSatisfy({ $0.isASCII && $0.isNumber }),
              let number = UInt64(value),
              number <= maximum else {
            return false
        }
        return permitsZero || number > 0
    }

    private static func hasValidSDPCharacters(_ sdp: String) -> Bool {
        sdp.unicodeScalars.allSatisfy { scalar in
            scalar.value == 10 || scalar.value == 13 || (0x20...0x7E).contains(scalar.value)
        }
    }

    private static func isCanonicalIPv4(_ address: String) -> Bool {
        let fields = address.split(separator: ".", omittingEmptySubsequences: false)
        guard fields.count == 4 else { return false }
        return fields.allSatisfy { field in
            guard !field.isEmpty,
                  field.allSatisfy({ $0.isASCII && $0.isNumber }),
                  field.count == 1 || field.first != "0",
                  let value = UInt8(field) else {
                return false
            }
            return String(value) == field
        }
    }

    private static func isValidMDNSName(_ address: String) -> Bool {
        guard address.hasSuffix(".local"), !address.hasSuffix("..local") else { return false }
        let label = address.dropLast(".local".count)
        guard (1...63).contains(label.count), label.first != "-", label.last != "-" else { return false }
        return label.allSatisfy { $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "-") }
    }
}
