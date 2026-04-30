import Foundation

@MainActor
final class LegacyMigrationPlanner {
    private let timestampReader = MediaTimestampReader()
    private let matcher = LiveBundleMatcher()
    private var perceptualDedupEnabled = false

    private struct ManifestSpec {
        let basePath: String              // dir above YYYY/
        let year: Int
        let month: Int
        let absolutePath: String          // full path to .watermelon_manifest.sqlite
        let monthDirAbsolutePath: String  // basePath/YYYY/MM (normalized)
    }

    private struct ScannerOutput {
        let usable: [LegacyFileCandidate]
        let unscheduled: [LegacyFileCandidate]
        let warnings: [String]
    }

    func scan(
        client: any RemoteStorageClientProtocol,
        rootPath: String,
        targetBasePath: String,
        enablePerceptualDedup: Bool
    ) async throws -> LegacyScanReport {
        try Task.checkCancellation()
        self.perceptualDedupEnabled = enablePerceptualDedup
        let allFiles = try await enumerate(client: client, root: rootPath)

        let manifestSpecs = detectManifestSpecs(in: allFiles)

        var warnings: [String] = []
        var manifestBundles: [LegacyAssetBundle] = []
        var ownedDirsByKey: [String: Set<String>] = [:]

        for spec in manifestSpecs {
            try Task.checkCancellation()
            do {
                guard let store = try await MonthManifestStore.loadManifestDirect(
                    client: client,
                    basePath: spec.basePath,
                    year: spec.year,
                    month: spec.month,
                    manifestAbsolutePath: spec.absolutePath,
                    pushSchemaUpgrade: false
                ) else {
                    warnings.append("Manifest \(spec.year)/\(spec.month) at \(spec.absolutePath) could not be opened")
                    ownedDirsByKey[spec.monthDirAbsolutePath.lowercased()] = []
                    continue
                }

                let snapshot = store.unsortedSnapshot()
                let resourcesByHash = Dictionary(uniqueKeysWithValues: snapshot.resources.map { ($0.contentHash, $0) })
                let linksByAsset = Dictionary(grouping: snapshot.links, by: \.assetFingerprint)

                ownedDirsByKey[spec.monthDirAbsolutePath.lowercased()] = Set(
                    snapshot.resources.map { RemoteFileNaming.collisionKey(for: $0.fileName) }
                )

                for asset in snapshot.assets {
                    let links = linksByAsset[asset.assetFingerprint] ?? []
                    if let bundle = buildManifestBundle(
                        asset: asset,
                        links: links,
                        resourcesByHash: resourcesByHash,
                        spec: spec
                    ) {
                        manifestBundles.append(bundle)
                    } else {
                        warnings.append("Manifest \(spec.year)/\(spec.month): asset fp:\(asset.assetFingerprint.hexString.prefix(8)) skipped (missing resource rows)")
                    }
                }
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                warnings.append("Manifest \(spec.year)/\(spec.month) read failed: \(error.localizedDescription)")
                ownedDirsByKey[spec.monthDirAbsolutePath.lowercased()] = []
            }
        }

        var scannerEntries: [RemoteStorageEntry] = []
        for entry in allFiles {
            try Task.checkCancellation()
            let parentDir = RemotePathBuilder.normalizePath(
                (entry.path as NSString).deletingLastPathComponent
            ).lowercased()

            if let owned = ownedDirsByKey[parentDir] {
                if entry.name == MonthManifestStore.manifestFileName { continue }
                let key = RemoteFileNaming.collisionKey(for: entry.name)
                if !owned.contains(key) {
                    warnings.append("Orphan: \(entry.path) is in a Watermelon month dir but not in its manifest")
                }
                continue
            }
            scannerEntries.append(entry)
        }

        let scannerOutput = try await runFileScanner(client: client, entries: scannerEntries)
        warnings.append(contentsOf: scannerOutput.warnings)

        let scannerSpecs = matcher.match(candidates: scannerOutput.usable)
        var scannerBundles: [LegacyAssetBundle] = []
        for spec in scannerSpecs {
            try Task.checkCancellation()
            scannerBundles.append(buildBundle(from: spec))
        }

        let rawPlans = bundlesByMonth(manifestBundles + scannerBundles)
        let classifiedPlans = try await classifyAgainstTarget(
            plans: rawPlans,
            client: client,
            targetBasePath: targetBasePath,
            warnings: &warnings
        )
        return LegacyScanReport(
            plans: classifiedPlans,
            unscheduledCandidates: scannerOutput.unscheduled,
            warnings: warnings
        )
    }

    // MARK: - Target classification

    private func classifyAgainstTarget(
        plans: [LegacyMonthPlan],
        client: any RemoteStorageClientProtocol,
        targetBasePath: String,
        warnings: inout [String]
    ) async throws -> [LegacyMonthPlan] {
        var result: [LegacyMonthPlan] = []
        for plan in plans {
            try Task.checkCancellation()
            let store: MonthManifestStore?
            do {
                store = try await MonthManifestStore.loadManifestOnlyIfExists(
                    client: client,
                    basePath: targetBasePath,
                    year: plan.month.year,
                    month: plan.month.month
                )
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                warnings.append("Could not classify against target manifest \(plan.month.text): \(error.localizedDescription)")
                result.append(plan)
                continue
            }

            guard let store else {
                // Target has no manifest for this month — every bundle is genuinely new.
                result.append(plan)
                continue
            }

            var targetDHashes: Set<Data> = []
            if perceptualDedupEnabled {
                do {
                    targetDHashes = try await buildTargetDHashSet(store: store, client: client)
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    warnings.append("Target dHash index for \(plan.month.text) failed: \(error.localizedDescription)")
                }
            }

            let updatedBundles = plan.bundles.map { bundle -> LegacyAssetBundle in
                var b = bundle
                b.action = computeAction(for: bundle, store: store, targetDHashes: targetDHashes)
                return b
            }
            result.append(LegacyMonthPlan(id: plan.id, month: plan.month, bundles: updatedBundles))
        }
        return result
    }

    private func buildTargetDHashSet(
        store: MonthManifestStore,
        client: any RemoteStorageClientProtocol
    ) async throws -> Set<Data> {
        let imageResources = store.unsortedSnapshot().resources.filter { resource in
            let ext = (resource.fileName as NSString).pathExtension.lowercased()
            return LegacyMediaExtensions.perceptualHashExtensions.contains(ext)
        }
        let cached = PerceptualHashCache.shared.lookupAll(
            contentHashes: imageResources.map(\.contentHash)
        )

        var result: Set<Data> = []
        for resource in imageResources {
            try Task.checkCancellation()
            if let dhash = cached[resource.contentHash] {
                result.insert(dhash)
                continue
            }
            let ext = (resource.fileName as NSString).pathExtension.lowercased()
            let absolutePath = RemotePathBuilder.absolutePath(
                basePath: store.basePath,
                remoteRelativePath: resource.remoteRelativePath
            )
            do {
                let dhash = try await withLocalReadURL(
                    client: client,
                    remotePath: absolutePath,
                    extensionHint: ext
                ) { url in
                    try DHashComputer.compute(url: url)
                }
                PerceptualHashCache.shared.store(contentHash: resource.contentHash, dhash: dhash)
                result.insert(dhash)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                // Single-resource failure shouldn't fail the whole month.
            }
        }
        return result
    }

    /// Materialize a remote file as a local URL for the duration of `body`. Uses the storage
    /// client's directReadURL when available (LocalVolume); otherwise downloads to a temp file
    /// that is removed on body exit, success or failure.
    private func withLocalReadURL<T>(
        client: any RemoteStorageClientProtocol,
        remotePath: String,
        extensionHint: String,
        body: (URL) async throws -> T
    ) async throws -> T {
        if let direct = await client.directReadURL(forRemotePath: remotePath) {
            return try await body(direct)
        }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("watermelon-legacy-\(UUID().uuidString)." + extensionHint)
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: remotePath, localURL: temp)
        return try await body(temp)
    }

    private func dhash(forContentHash hash: Data, fileURL: URL) -> Data? {
        if let cached = PerceptualHashCache.shared.lookup(contentHash: hash) {
            return cached
        }
        guard let computed = try? DHashComputer.compute(url: fileURL) else { return nil }
        PerceptualHashCache.shared.store(contentHash: hash, dhash: computed)
        return computed
    }

    private func computeAction(
        for bundle: LegacyAssetBundle,
        store: MonthManifestStore,
        targetDHashes: Set<Data>
    ) -> LegacyBundleAction {
        if store.containsAssetFingerprint(bundle.assetFingerprint) {
            return .skipExactMatch
        }
        let resources = bundle.resources.map { (role: $0.role, slot: $0.slot, hash: $0.contentHash) }
        if store.findEnclosingAssetFingerprint(forResources: resources) != nil {
            return .skipEnclosed
        }
        // Manifest-driven bundles carry authoritative role assignments — never drop perceptually.
        if bundle.source == .scanner, !targetDHashes.isEmpty {
            for component in bundle.resources {
                guard let bundleDhash = component.dhash else { continue }
                if targetDHashes.contains(where: { DHashComputer.hammingDistance(bundleDhash, $0) <= 5 }) {
                    return .skipPerceptualDuplicate
                }
            }
        }
        let subsets = store.findStrictSubsetAssetFingerprints(forResources: resources)
        if !subsets.isEmpty {
            return .replacesSubsets(count: subsets.count)
        }
        return .insertNew
    }

    // MARK: - Manifest detection

    private func detectManifestSpecs(in files: [RemoteStorageEntry]) -> [ManifestSpec] {
        var result: [ManifestSpec] = []
        for entry in files where !entry.isDirectory && entry.name == MonthManifestStore.manifestFileName {
            guard let spec = parseManifestSpec(absolutePath: entry.path) else { continue }
            result.append(spec)
        }
        return result.sorted { lhs, rhs in
            if lhs.year != rhs.year { return lhs.year < rhs.year }
            return lhs.month < rhs.month
        }
    }

    private func parseManifestSpec(absolutePath: String) -> ManifestSpec? {
        let normalized = RemotePathBuilder.normalizePath(absolutePath)
        let monthDir = (normalized as NSString).deletingLastPathComponent
        let monthComponent = (monthDir as NSString).lastPathComponent
        let yearDir = (monthDir as NSString).deletingLastPathComponent
        let yearComponent = (yearDir as NSString).lastPathComponent
        let basePath = (yearDir as NSString).deletingLastPathComponent
        guard let year = Int(yearComponent), (1900...9999).contains(year) else { return nil }
        guard let month = Int(monthComponent), (1...12).contains(month) else { return nil }
        let normalizedBase = basePath.isEmpty ? "/" : basePath
        return ManifestSpec(
            basePath: RemotePathBuilder.normalizePath(normalizedBase),
            year: year,
            month: month,
            absolutePath: normalized,
            monthDirAbsolutePath: RemotePathBuilder.normalizePath(monthDir)
        )
    }

    private func buildManifestBundle(
        asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        resourcesByHash: [Data: RemoteManifestResource],
        spec: ManifestSpec
    ) -> LegacyAssetBundle? {
        guard !links.isEmpty else { return nil }
        var components: [LegacyResourceComponent] = []
        for link in links {
            guard let resource = resourcesByHash[link.resourceHash] else { return nil }
            let remotePath = RemotePathBuilder.absolutePath(
                basePath: spec.basePath,
                remoteRelativePath: resource.remoteRelativePath
            )
            components.append(LegacyResourceComponent(
                role: link.role,
                slot: link.slot,
                remotePath: remotePath,
                originalFilename: resource.fileName,
                fileSize: resource.fileSize,
                contentHash: resource.contentHash,
                dhash: nil   // manifest-driven bundles aren't subject to perceptual dedup
            ))
        }

        let creationDate: Date? = asset.creationDateMs.map { Date(millisecondsSinceEpoch: $0) }
        return LegacyAssetBundle(
            kind: deriveKind(fromRoles: links.map { $0.role }),
            source: .manifest,
            creationDate: creationDate,
            timestampSource: .unknown,
            resources: components,
            assetFingerprint: asset.assetFingerprint,
            preferredMonth: LibraryMonthKey(year: spec.year, month: spec.month)
        )
    }

    private func deriveKind(fromRoles roles: [Int]) -> LegacyBundleKind {
        if roles.contains(where: { ResourceTypeCode.isPairedVideo($0) }) { return .livePhoto }
        if roles.contains(where: { ResourceTypeCode.isVideoLike($0) }) { return .video }
        return .photo
    }

    // MARK: - Per-file scanner

    private func runFileScanner(
        client: any RemoteStorageClientProtocol,
        entries: [RemoteStorageEntry]
    ) async throws -> ScannerOutput {
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

            var dhash: Data?
            if perceptualDedupEnabled, let hash, LegacyMediaExtensions.perceptualHashExtensions.contains(ext) {
                dhash = self.dhash(forContentHash: hash, fileURL: readURL)
                if dhash == nil {
                    warnings.append("Failed to dHash \(entry.name)")
                }
            }

            if needsCleanup {
                try? FileManager.default.removeItem(at: readURL)
            }

            let sanitized = RemotePathBuilder.sanitizeFilename(entry.name)
            let stem = (sanitized as NSString).deletingPathExtension
            let parentDir = RemotePathBuilder.normalizePath(
                (entry.path as NSString).deletingLastPathComponent
            )

            candidates.append(
                LegacyFileCandidate(
                    remotePath: entry.path,
                    parentDirectory: parentDir,
                    sanitizedStem: stem,
                    originalFilename: sanitized,
                    lowercasedExtension: ext,
                    kind: kind,
                    fileSize: size,
                    timestamp: timestamp.date,
                    timestampSource: timestamp.source,
                    contentHash: hash,
                    dhash: dhash
                )
            )
        }

        let usable = candidates.filter { $0.timestamp != nil && $0.contentHash != nil }
        let unscheduled = candidates.filter { $0.timestamp == nil || $0.contentHash == nil }
        return ScannerOutput(usable: usable, unscheduled: unscheduled, warnings: warnings)
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
                contentHash: hash,
                dhash: entry.candidate.dhash
            )
        }

        let fingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: components.map { (role: $0.role, slot: $0.slot, contentHash: $0.contentHash) }
        )

        return LegacyAssetBundle(
            kind: spec.kind,
            source: .scanner,
            creationDate: spec.creationDate,
            timestampSource: spec.timestampSource,
            resources: components,
            assetFingerprint: fingerprint,
            preferredMonth: nil
        )
    }

    private func bundlesByMonth(_ bundles: [LegacyAssetBundle]) -> [LegacyMonthPlan] {
        let grouped = Dictionary(grouping: bundles) { bundle in
            bundle.preferredMonth ?? LibraryMonthKey.from(date: bundle.creationDate)
        }
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
