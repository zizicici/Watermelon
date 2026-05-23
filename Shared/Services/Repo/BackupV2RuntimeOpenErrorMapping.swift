import Foundation

enum BackupV2RuntimeOpenFailureKind: Equatable, Sendable {
    case unsupportedRemoteFormat(minAppVersion: String?)
    case repoIdentityMismatch
    case requiresForegroundMigration
    case repoFormatRegression
    case damagedV2Repo
    case profileMissingID
    case cancellation
    case transientRemoteFailure
    case other
}

struct BackupV2RuntimeOpenFailure {
    let kind: BackupV2RuntimeOpenFailureKind
    let originalError: Error
}

enum BackupV2RuntimeOpenErrorMapping {
    static func withOpenErrorNormalization<T>(
        _ operation: () async throws -> T
    ) async throws -> T {
        do {
            return try await operation()
        } catch {
            throw normalizeOpenError(error)
        }
    }

    static func normalizeOpenError(_ error: Error) -> Error {
        if RemoteWriteClassifier.isCancellation(error) {
            return CancellationError()
        }
        if let buildError = error as? BackupV2RuntimeBuildError {
            return buildError
        }
        if let bootstrap = error as? RepoBootstrap.BootstrapError {
            return translate(bootstrapError: bootstrap)
        }
        if let conflict = error as? RepoBootstrap.VersionConflict {
            return translate(versionConflict: conflict)
        }
        if let compatibility = error as? BackupCompatibilityError {
            switch compatibility {
            case .remoteFormatUnsupported(let minAppVersion):
                return BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minAppVersion)
            case .damagedV2Repo:
                return BackupV2RuntimeBuildError.damagedV2Repo
            case .repoIdentityMismatch(let stored, let observed):
                return BackupV2RuntimeBuildError.repoIdentityMismatch(
                    stored: stored ?? "",
                    observed: observed ?? ""
                )
            case .requiresForegroundMigration:
                return BackupV2RuntimeBuildError.requiresForegroundMigration
            case .repoFormatRegression(let repoID):
                return BackupV2RuntimeBuildError.repoFormatRegression(repoID: repoID ?? "")
            }
        }
        return error
    }

    static func translate(bootstrapError: RepoBootstrap.BootstrapError) -> Error {
        switch bootstrapError {
        case .ioFailure(let underlying):
            if RemoteWriteClassifier.isCancellation(underlying) {
                return CancellationError()
            }
            // Surface external-volume loss directly; the run-level classifier doesn't peel BootstrapError.
            if RemoteStorageClientError.isLikelyExternalStorageUnavailable(underlying) {
                return underlying
            }
            return BackupV2RuntimeBuildError.damagedV2Repo
        case .futureFormatVersion(let minAppVersion):
            return BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minAppVersion)
        }
    }

    static func translate(versionConflict: RepoBootstrap.VersionConflict) -> Error {
        switch versionConflict {
        case .higherFormatVersion(_, _, let minAppVersion),
             .mismatchedFormatVersion(_, _, let minAppVersion):
            return BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minAppVersion)
        case .unreadable(let underlying):
            if let underlying, RemoteWriteClassifier.isCancellation(underlying) {
                return CancellationError()
            }
            if let underlying,
               RemoteStorageClientError.isLikelyExternalStorageUnavailable(underlying) {
                return underlying
            }
            return BackupV2RuntimeBuildError.damagedV2Repo
        }
    }

    static func translateToCompatibilityError(bootstrapError: RepoBootstrap.BootstrapError) -> Error {
        let translated = translate(bootstrapError: bootstrapError)
        guard let build = translated as? BackupV2RuntimeBuildError else { return translated }
        return compatibilityError(for: classifyBuildFailure(build))
    }

    static func translateToCompatibilityError(versionConflict: RepoBootstrap.VersionConflict) -> Error {
        let translated = translate(versionConflict: versionConflict)
        guard let build = translated as? BackupV2RuntimeBuildError else { return translated }
        return compatibilityError(for: classifyBuildFailure(build))
    }

    static func classifyBuildFailure(_ error: Error) -> BackupV2RuntimeOpenFailure {
        if RemoteWriteClassifier.isCancellation(error) {
            return BackupV2RuntimeOpenFailure(kind: .cancellation, originalError: error)
        }
        if let buildError = error as? BackupV2RuntimeBuildError {
            switch buildError {
            case .unsupportedRemoteFormat(let minAppVersion):
                return BackupV2RuntimeOpenFailure(
                    kind: .unsupportedRemoteFormat(minAppVersion: minAppVersion),
                    originalError: error
                )
            case .repoIdentityMismatch:
                return BackupV2RuntimeOpenFailure(kind: .repoIdentityMismatch, originalError: error)
            case .requiresForegroundMigration:
                return BackupV2RuntimeOpenFailure(kind: .requiresForegroundMigration, originalError: error)
            case .repoFormatRegression:
                return BackupV2RuntimeOpenFailure(kind: .repoFormatRegression, originalError: error)
            case .damagedV2Repo:
                return BackupV2RuntimeOpenFailure(kind: .damagedV2Repo, originalError: error)
            case .profileMissingID:
                return BackupV2RuntimeOpenFailure(kind: .profileMissingID, originalError: error)
            }
        }
        if RemoteWriteClassifier.isTransientVerifyFailure(error) {
            return BackupV2RuntimeOpenFailure(kind: .transientRemoteFailure, originalError: error)
        }
        return BackupV2RuntimeOpenFailure(kind: .other, originalError: error)
    }

    static func compatibilityError(for failure: BackupV2RuntimeOpenFailure) -> Error {
        switch failure.kind {
        case .unsupportedRemoteFormat(let minAppVersion):
            return BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
        case .repoIdentityMismatch:
            if case BackupV2RuntimeBuildError.repoIdentityMismatch(let stored, let observed) = failure.originalError {
                return BackupCompatibilityError.repoIdentityMismatch(
                    stored: stored.isEmpty ? nil : stored,
                    observed: observed.isEmpty ? nil : observed
                )
            }
            return BackupCompatibilityError.repoIdentityMismatch(stored: nil, observed: nil)
        case .requiresForegroundMigration:
            return BackupCompatibilityError.requiresForegroundMigration
        case .repoFormatRegression:
            if case BackupV2RuntimeBuildError.repoFormatRegression(let repoID) = failure.originalError {
                return BackupCompatibilityError.repoFormatRegression(repoID: repoID.isEmpty ? nil : repoID)
            }
            return BackupCompatibilityError.repoFormatRegression(repoID: nil)
        case .damagedV2Repo:
            return BackupCompatibilityError.damagedV2Repo
        case .profileMissingID:
            return profileMissingIDCompatibilityError()
        case .cancellation:
            return CancellationError()
        case .transientRemoteFailure, .other:
            return failure.originalError
        }
    }

    static func withCompatibilityMapping<T>(
        metadataClient: (any RemoteStorageClientProtocol)?,
        disconnectOnError: Bool,
        _ operation: () async throws -> T
    ) async throws -> T {
        do {
            return try await operation()
        } catch {
            if disconnectOnError {
                await metadataClient?.disconnectSafely()
            }
            let failure = classifyBuildFailure(error)
            throw compatibilityError(for: failure)
        }
    }

    static func profileMissingIDCompatibilityError() -> NSError {
        NSError(
            domain: "BackupRunPreparation",
            code: -90,
            userInfo: [NSLocalizedDescriptionKey: "profile missing id — cannot prepare V2 runtime"]
        )
    }
}
