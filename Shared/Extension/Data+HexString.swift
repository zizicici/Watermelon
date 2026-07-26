import Foundation

private let lowercaseHexDigits = Array("0123456789abcdef".utf8)

extension Data {
    var hexString: String {
        String(unsafeUninitializedCapacity: count * 2) { output in
            var outputIndex = 0
            for byte in self {
                output[outputIndex] = lowercaseHexDigits[Int(byte >> 4)]
                output[outputIndex + 1] = lowercaseHexDigits[Int(byte & 0x0F)]
                outputIndex += 2
            }
            return outputIndex
        }
    }
}
