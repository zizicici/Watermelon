import Foundation

@MainActor
final class LegacyMigrationPlanner {
    private let timestampReader = MediaTimestampReader()
    private let matcher = LiveBundleMatcher()

    func scan(client: any RemoteStorageClientProtocol, rootPath: String) async throws -> LegacyScanReport {
        try Task.checkCancellation()

        let entries = try await enumerate(client: client, root: rootPath)
        var candidates: [LegacyFileCandidate] = []
        var warnings: [String] = []

        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("watermelon-legacy-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        for entry in entries {
            try Task.checkCancellation()
            let ext = (entry.name as NSString).pathExtension.lowercased()
            guard let kind = LegacyMediaExtensions.kind(forExtension: ext) else { continue }

            // Fast path: storage already keeps the file on disk (e.g. external volume) — read in place,
            // no copy to /tmp. Otherwise download a single temp copy that feeds both metadata + hash.
            let readURL: URL
            let needsCleanup: Bool
            if let direct = await client.directReadURL(forRemotePath: entry.path) {
                readURL = direct
                needsCleanup = false
            } else {
                let temp = tempDir.appendingPathComponent(UUID().uuidString + "." + ext)
                do {
                    try await client.download(remotePath: entry.path, localURL: temp)
                } catch {
                    warnings.append("Failed to read \(entry.name): \(error.localizedDescription)")
                    continue
                }
                readURL = temp
                needsCleanup = true
            }

            let timestamp = await timestampReader.read(url: readURL, kind: kind, fallbackMtime: entry.modificationDate)

            var hash: Data?
            var size: Int64 = entry.size
            if timestamp.date != nil {
                do {
                    let result = try FileHasher.sha256(of: readURL)
                    hash = result.hash
                    if result.size > 0 { size = result.size }
                } catch {
                    warnings.append("Failed to hash \(entry.name): \(error.localizedDescription)")
                }
            }
            if needsCleanup {
                try? FileManager.default.removeItem(at: readURL)
            }

            let sanitized = RemotePathBuilder.sanitizeFilename(entry.name)
            let stem = (sanitized as NSString).deletingPathExtension

            candidates.append(
                LegacyFileCandidate(
                    remotePath: entry.path,
                    sanitizedStem: stem,
                    originalFilename: sanitized,
                    lowercasedExtension: ext,
                    kind: kind,
                    fileSize: size,
                    timestamp: timestamp.date,
                    timestampSource: timestamp.source,
                    contentHash: hash
                )
            )
        }

        let usable = candidates.filter { $0.timestamp != nil && $0.contentHash != nil }
        let unscheduled = candidates.filter { $0.timestamp == nil || $0.contentHash == nil }

        let bundleSpecs = matcher.match(candidates: usable)

        var bundles: [LegacyAssetBundle] = []
        for spec in bundleSpecs {
            try Task.checkCancellation()
            bundles.append(buildBundle(from: spec))
        }

        let plans = bundlesByMonth(bundles)
        return LegacyScanReport(plans: plans, unscheduledCandidates: unscheduled, warnings: warnings)
    }

    // MARK: - Recursive listing

    private func enumerate(
        client: any RemoteStorageClientProtocol,
        root: String
    ) async throws -> [RemoteStorageEntry] {
        let normalizedRoot = RemotePathBuilder.normalizePath(root)
        var pending: [String] = [normalizedRoot]
        var files: [RemoteStorageEntry] = []
        while let dir = pending.popLast() {
            try Task.checkCancellation()
            let entries = try await client.list(path: dir)
            for entry in entries {
                if entry.name == "." || entry.name == ".." { continue }
                if entry.isDirectory {
                    pending.append(entry.path)
                } else {
                    files.append(entry)
                }
            }
        }
        return files
    }

    private func buildBundle(from spec: LegacyMatchedBundleSpec) -> LegacyAssetBundle {
        let components: [LegacyResourceComponent] = spec.components.compactMap { entry in
            guard let hash = entry.candidate.contentHash else { return nil }
            return LegacyResourceComponent(
                role: entry.role,
                slot: entry.slot,
                remotePath: entry.candidate.remotePath,
                originalFilename: entry.candidate.originalFilename,
                fileSize: entry.candidate.fileSize,
                contentHash: hash
            )
        }

        let fingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: components.map { (role: $0.role, slot: $0.slot, contentHash: $0.contentHash) }
        )

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
