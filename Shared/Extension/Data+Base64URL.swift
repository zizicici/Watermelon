import Foundation

extension Data {
    init?(base64URLEncoded value: String) {
        guard value.range(
            of: "^[A-Za-z0-9_-]*$",
            options: .regularExpression
        ) != nil else {
            return nil
        }
        let base64 = value
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
            .padding(toMultipleOf: 4, with: "=")
        self.init(base64Encoded: base64)
    }

    func base64URLEncodedString() -> String {
        base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }
}

private extension String {
    func padding(
        toMultipleOf divisor: Int,
        with character: Character
    ) -> String {
        let remainder = count % divisor
        guard remainder != 0 else { return self }
        return self + String(
            repeating: character,
            count: divisor - remainder
        )
    }
}
