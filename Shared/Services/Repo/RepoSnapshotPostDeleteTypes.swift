import Foundation

enum RepoSnapshotPostDeleteVerificationResult: Equatable, Sendable {
    case passed(evidence: RepoSnapshotPostDeleteVerificationEvidence)
    case failed(reason: RepoSnapshotPostDeleteVerificationFailure, evidence: RepoSnapshotPostDeleteVerificationEvidence?)
    case inconclusive(reason: RepoSnapshotPostDeleteVerificationInconclusive)
}

struct RepoSnapshotPostDeleteVerificationEvidence: Equatable, Sendable {
    let acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo
    let materializedCovered: CoveredRanges
    let observedSeqByWriter: [String: UInt64]
    let observedClock: UInt64
    let validatedProtectedSnapshots: [String]
}

enum RepoSnapshotPostDeleteVerificationFailure: Equatable, Sendable {
    case missingRepoIdentity(expected: String)
    case repoIdentityMismatch(expected: String, observed: String)
    case missingAcceptedSnapshot(month: LibraryMonthKey)
    case acceptedSnapshotCoverageRegression(filename: String)
    case stateNotRetentionSuperset
    case coveredRangeRegression
    case observedSeqRegression(writerID: String, expectedAtLeast: UInt64, observed: UInt64)
    case observedClockRegression(expectedAtLeast: UInt64, observed: UInt64)
    case acceptedSnapshotSupersedeUnsafe(expectedFilename: String, observedFilename: String, observedLamport: UInt64)
    case acceptedSnapshotContentMismatch(filename: String, expectedSHA: String, observedSHA: String)
    case protectedSnapshotMissingOrTampered(filename: String, expectedSHA: String, observedSHA: String?)
}

enum RepoSnapshotPostDeleteVerificationInconclusive: Equatable, Sendable {
    case repoIdentityReadFailed
    case materializerReadRace
    case materializerReadFailed
    case protectedSnapshotReadFailed(filename: String)
    case deleteTargetStillPresent(path: String)
    case cancelled
}

struct RepoSnapshotPostDeleteEquivalenceContract: Equatable, Sendable {
    let acceptedSnapshotFilename: String
    let acceptedSnapshotLamport: UInt64
    let acceptedSnapshotSHA256Hex: String
    let acceptedSnapshotCovered: CoveredRanges
    let additionalProtectedSnapshotSHA256ByFilename: [String: String]
    let requiredObservedSeqByWriter: [String: UInt64]
    let preDeleteCovered: CoveredRanges
    let preDeleteState: RepoSnapshotState
    let preDeleteObservedClock: UInt64
}
