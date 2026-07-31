import Foundation

enum MacPhotoLibraryDeletionPreparationDisposition: Equatable {
    case proceed
    case cancel
    case stale
}

enum MacPhotoLibraryDeletionPreparationPolicy {
    static func disposition(
        isCancelled: Bool,
        isStillValid: Bool
    ) -> MacPhotoLibraryDeletionPreparationDisposition {
        if isCancelled {
            return .cancel
        }
        return isStillValid ? .proceed : .stale
    }

    static func ensureCommitAllowed(
        isCancelled: Bool
    ) throws {
        guard !isCancelled else {
            throw CancellationError()
        }
    }
}
