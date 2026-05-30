import Foundation

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
            return mapUnreadableUnderlying(underlying)
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
            guard let underlying else { return BackupV2RuntimeBuildError.damagedV2Repo }
            return mapUnreadableUnderlying(underlying)
        }
    }

    /// Shared cancellation/external-volume/transient ladder for unreadable bootstrap/version I/O.
    /// External-volume loss is surfaced directly because the run-level classifier doesn't peel BootstrapError.
    private static func mapUnreadableUnderlying(_ underlying: Error) -> Error {
        if RemoteWriteClassifier.isCancellation(underlying) {
            return CancellationError()
        }
        if RemoteStorageClientError.isLikelyExternalStorageUnavailable(underlying) {
            return underlying
        }
        if RemoteWriteClassifier.isTransientVerifyFailure(underlying) {
            return underlying
        }
        return BackupV2RuntimeBuildError.damagedV2Repo
    }

    static func translateToCompatibilityError(bootstrapError: RepoBootstrap.BootstrapError) -> Error {
        let translated = translate(bootstrapError: bootstrapError)
        guard translated is BackupV2RuntimeBuildError else { return translated }
        return compatibilityError(for: translated)
    }

    static func translateToCompatibilityError(versionConflict: RepoBootstrap.VersionConflict) -> Error {
        let translated = translate(versionConflict: versionConflict)
        guard translated is BackupV2RuntimeBuildError else { return translated }
        return compatibilityError(for: translated)
    }

    /// Maps an open/build error to its user-facing compatibility error: cancellation collapses to
    /// `CancellationError`, `BackupV2RuntimeBuildError` cases map to `BackupCompatibilityError`, and
    /// transient/unknown errors pass through unchanged.
    static func compatibilityError(for error: Error) -> Error {
        if RemoteWriteClassifier.isCancellation(error) {
            return CancellationError()
        }
        guard let buildError = error as? BackupV2RuntimeBuildError else {
            return error
        }
        switch buildError {
        case .unsupportedRemoteFormat(let minAppVersion):
            return BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
        case .repoIdentityMismatch(let stored, let observed):
            return BackupCompatibilityError.repoIdentityMismatch(
                stored: stored.isEmpty ? nil : stored,
                observed: observed.isEmpty ? nil : observed
            )
        case .requiresForegroundMigration:
            return BackupCompatibilityError.requiresForegroundMigration
        case .repoFormatRegression(let repoID):
            return BackupCompatibilityError.repoFormatRegression(repoID: repoID.isEmpty ? nil : repoID)
        case .damagedV2Repo:
            return BackupCompatibilityError.damagedV2Repo
        case .profileMissingID:
            return profileMissingIDCompatibilityError()
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
            throw compatibilityError(for: error)
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
