import CryptoKit
import Foundation

/// Additive, write-side authentication of a snapshot's covered ranges. The header carries the version
/// marker; the filename carries a SHA-256 digest binding snapshot identity to its canonical covered. It
/// exists solely so corrupt-repair can prove a body-corrupt snapshot was not the sole record of a GC'd
/// covered prefix — it is never an accepted baseline, covered-max, or authority signal.
struct SnapshotCoverageAttestation: Equatable, Sendable {
    static let currentVersion = 1
    let version: Int

    init(version: Int = SnapshotCoverageAttestation.currentVersion) {
        self.version = version
    }
}

enum SnapshotCoverageDigest {
    /// Domain tag specific to snapshot coverage attestation — prevents a digest from being reused as any
    /// other attestation preimage.
    static let domainTag = "watermelon.snapshot.coverage.attestation"

    /// Deterministic per-writer covered serialization: writer IDs lexicographically sorted, each writer's
    /// merged ranges (already ascending) emitted as `low-high` pairs. Writers with no ranges are dropped so
    /// an empty-coverage writer cannot shift the digest.
    static func canonicalCovered(_ covered: CoveredRanges) -> String {
        covered.rangesByWriter.keys.sorted().compactMap { writer -> String? in
            guard let ranges = covered.rangesByWriter[writer], !ranges.isEmpty else { return nil }
            let pairs = ranges.map { "\($0.low)-\($0.high)" }.joined(separator: ",")
            return "\(writer)=\(pairs)"
        }.joined(separator: ";")
    }

    static func digest(
        version: Int,
        repoID: String,
        month: LibraryMonthKey,
        writerID: String,
        filenameLamport: UInt64,
        filenameRunIDPrefix: String,
        covered: CoveredRanges
    ) -> String {
        let preimage = [
            "v=\(version)",
            "domain=\(domainTag)",
            "repoID=\(repoID)",
            "scope=\(CommitHeader.monthScope(month))",
            "writerID=\(writerID)",
            "lamport=\(RepoLayout.format16Hex(filenameLamport))",
            "runIDPrefix=\(filenameRunIDPrefix)",
            "covered=\(canonicalCovered(covered))"
        ].joined(separator: "\n")
        let hash = SHA256.hash(data: Data(preimage.utf8))
        return Data(hash).hexString
    }

    /// The filename digest for an attested header, or nil for a legacy (unattested) header so the writer
    /// keeps emitting the legacy 4-segment filename.
    static func filenameDigest(
        forHeader header: SnapshotHeader,
        month: LibraryMonthKey,
        lamport: UInt64,
        runIDPrefix: String
    ) -> String? {
        guard let attestation = header.coverageAttestation else { return nil }
        return digest(
            version: attestation.version,
            repoID: header.repoID,
            month: month,
            writerID: header.writerID,
            filenameLamport: lamport,
            filenameRunIDPrefix: runIDPrefix,
            covered: header.covered
        )
    }
}
