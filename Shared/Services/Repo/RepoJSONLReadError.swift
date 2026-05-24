import Foundation

enum RepoJSONLReadError: Error {
    case missingHeader
    case missingEnd
    case integrityMismatch(IntegrityResult)
    case decodeFailure(Error)
    case notFound(filename: String)
}
