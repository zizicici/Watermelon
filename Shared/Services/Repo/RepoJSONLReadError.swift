import Foundation

enum RepoJSONLReadError: Error {
    case missingHeader
    case missingEnd
    case integrityMismatch(IntegrityResult)
    case decodeFailure(Error)
    case notFound(filename: String)
}

extension IntegrityAccumulator {
    func verifyOrThrowJSONLMismatch(expectedSha256: String, expectedRowCount: Int) throws {
        let result = verifyIntegrity(
            expectedSha256: expectedSha256,
            expectedRowCount: expectedRowCount,
            actualSha256: finalize(),
            actualRowCount: rowCount
        )
        if result != .ok {
            throw RepoJSONLReadError.integrityMismatch(result)
        }
    }
}
