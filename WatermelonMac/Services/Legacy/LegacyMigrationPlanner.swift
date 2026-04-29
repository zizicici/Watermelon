import Foundation

@MainActor
final class LegacyMigrationPlanner {
    private let scanner = LegacyFolderScanner()
    private let timestampReader = MediaTimestampReader()
    private let matcher = LiveBundleMatcher()

    func scan(rootURL: URL) async throws -> LegacyScanReport {
        try Task.checkCancellation()

        let scanned = try scanner.enumerate(at: rootURL)
        var candidates: [LegacyFileCandidate] = []
        var warnings: [String] = []

        for file in scanned {
            try Task.checkCancellation()
            let ext = file.url.pathExtension.lowercased()
            guard let kind = LegacyMediaExtensions.kind(forExtension: ext) else { continue }

            let timestamp = await timestampReader.read(url: file.url, kind: kind, fallbackMtime: file.mtime)

            let original = file.url.lastPathComponent
            let sanitizedFull = RemotePathBuilder.sanitizeFilename(original)
            let stem = (sanitizedFull as NSString).deletingPathExtension

            candidates.append(
                LegacyFileCandidate(
                    url: file.url,
                    sanitizedStem: stem,
                    originalFilename: sanitizedFull,
                    lowercasedExtension: ext,
                    kind: kind,
                    fileSize: file.fileSize,
                    timestamp: timestamp.date,
                    timestampSource: timestamp.source
                )
            )
        }

        // Skip candidates that lack any timestamp — never silently bucket into 1970.
        let usable = candidates.filter { $0.timestamp != nil }
        let unscheduled = candidates.filter { $0.timestamp == nil }

        let bundleSpecs = matcher.match(candidates: usable)

        var bundles: [LegacyAssetBundle] = []
        for spec in bundleSpecs {
            try Task.checkCancellation()
            do {
                let bundle = try await buildBundle(from: spec)
                bundles.append(bundle)
            } catch {
                warnings.append("Failed to hash bundle for \(spec.components.first?.candidate.url.lastPathComponent ?? "unknown"): \(error.localizedDescription)")
            }
        }

        let plans = bundlesByMonth(bundles)
        return LegacyScanReport(plans: plans, unscheduledCandidates: unscheduled, warnings: warnings)
    }

    private func buildBundle(from spec: LegacyMatchedBundleSpec) async throws -> LegacyAssetBundle {
        var components: [LegacyResourceComponent] = []
        components.reserveCapacity(spec.components.count)

        for entry in spec.components {
            try Task.checkCancellation()
            let (hash, size) = try FileHasher.sha256(of: entry.candidate.url)
            components.append(
                LegacyResourceComponent(
                    role: entry.role,
                    slot: entry.slot,
                    url: entry.candidate.url,
                    originalFilename: entry.candidate.originalFilename,
                    fileSize: size > 0 ? size : entry.candidate.fileSize,
                    contentHash: hash
                )
            )
        }

        let fingerprintTuples = components.map { (role: $0.role, slot: $0.slot, contentHash: $0.contentHash) }
        let fingerprint = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: fingerprintTuples)

        return LegacyAssetBundle(
            kind: spec.kind,
            creationDate: spec.creationDate,
            timestampSource: spec.timestampSource,
            resources: components,
            assetFingerprint: fingerprint
        )
    }

    private func bundlesByMonth(_ bundles: [LegacyAssetBundle]) -> [LegacyMonthPlan] {
        let grouped = Dictionary(grouping: bundles) { LibraryMonthKey.from(date: $0.creationDate) }
        let sortedKeys = grouped.keys.sorted()
        return sortedKeys.map { key in
            let monthBundles = (grouped[key] ?? []).sorted { lhs, rhs in
                if lhs.creationDate != rhs.creationDate {
                    return (lhs.creationDate ?? .distantPast) < (rhs.creationDate ?? .distantPast)
                }
                return lhs.assetFingerprint.hexString < rhs.assetFingerprint.hexString
            }
            return LegacyMonthPlan(id: key, month: key, bundles: monthBundles)
        }
    }
}
