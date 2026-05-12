import Foundation

enum RemoteFileNaming {
    struct ResourceIdentity: Equatable {
        let role: Int
        let slot: Int
        let originalFilename: String
    }

    private static let assetStemRolePriority: [Int] = [
        ResourceTypeCode.photo,
        ResourceTypeCode.video,
        ResourceTypeCode.fullSizePhoto,
        ResourceTypeCode.fullSizeVideo,
        ResourceTypeCode.alternatePhoto,
        ResourceTypeCode.pairedVideo
    ]

    private static let primaryRoles: Set<Int> = [
        ResourceTypeCode.photo,
        ResourceTypeCode.video,
        ResourceTypeCode.pairedVideo
    ]

    static func preferredAssetNameStem(
        orderedResources: [ResourceIdentity],
        fallbackTimestampMs: Int64?
    ) -> String {
        for role in assetStemRolePriority {
            if let preferred = orderedResources.first(where: { $0.role == role && $0.slot == 0 }) {
                let stem = sanitizedFileStem(from: preferred.originalFilename)
                if !stem.isEmpty {
                    return stem
                }
            }
        }

        if let first = orderedResources.first {
            let stem = sanitizedFileStem(from: first.originalFilename)
            if !stem.isEmpty {
                return stem
            }
        }

        return "asset_\(fallbackTimestampMs ?? 0)"
    }

    static func preferredRemoteFileName(
        preferredAssetNameStem: String,
        resource: ResourceIdentity
    ) -> String {
        let sanitizedOriginalName = RemotePathBuilder.sanitizeFilename(resource.originalFilename)
        let originalExt = (sanitizedOriginalName as NSString).pathExtension
        let originalStem = sanitizedFileStem(from: resource.originalFilename)

        let baseStem: String = {
            if !preferredAssetNameStem.isEmpty {
                return preferredAssetNameStem
            }
            let fallback = sanitizedFileStem(from: resource.originalFilename)
            return fallback.isEmpty ? "resource" : fallback
        }()

        let isPrimary = resource.slot == 0 && primaryRoles.contains(resource.role)
        let stem: String
        if isPrimary {
            stem = baseStem
        } else {
            var detailStem = originalStem
            if detailStem.isEmpty {
                detailStem = fallbackResourceLabel(forRole: resource.role)
            }

            let baseLower = baseStem.lowercased()
            let detailLower = detailStem.lowercased()

            if detailLower == baseLower {
                detailStem = fallbackResourceLabel(forRole: resource.role)
            }

            let updatedDetailLower = detailStem.lowercased()
            if updatedDetailLower.hasPrefix(baseLower + "_") ||
                updatedDetailLower.hasPrefix(baseLower + "-") ||
                updatedDetailLower == baseLower {
                stem = detailStem
            } else {
                stem = "\(baseStem)_\(detailStem)"
            }
        }

        if originalExt.isEmpty {
            return stem
        }
        return "\(stem).\(originalExt)"
    }

    static func fallbackResourceLabel(forRole role: Int) -> String {
        let raw = resourceTypeRawName(forRole: role)
        let separatedCamel = raw.replacingOccurrences(
            of: "([a-z0-9])([A-Z])",
            with: "$1 $2",
            options: .regularExpression
        )
        let normalized = separatedCamel.replacingOccurrences(
            of: "[^A-Za-z0-9]+",
            with: " ",
            options: .regularExpression
        )
        let words = normalized
            .split(separator: " ")
            .map { token in
                token.prefix(1).uppercased() + token.dropFirst()
            }
        if words.isEmpty {
            return "Resource\(max(role, 0))"
        }
        return words.joined()
    }

    static func sanitizedFileStem(from originalFilename: String) -> String {
        let sanitized = RemotePathBuilder.sanitizeFilename(originalFilename)
        return (sanitized as NSString).deletingPathExtension
    }

    static func resolveNextAvailableName(
        baseName: String,
        occupiedNames: Set<String>,
        writerID: String? = nil,
        forceWriterIDSuffix: Bool = false
    ) -> String {
        resolveNextAvailableName(
            baseName: baseName,
            collisionKeys: collisionKeySet(from: occupiedNames),
            writerID: writerID,
            forceWriterIDSuffix: forceWriterIDSuffix
        )
    }

    static func resolveNextAvailableName(
        baseName: String,
        collisionKeys: Set<String>,
        writerID: String? = nil,
        forceWriterIDSuffix: Bool = false
    ) -> String {
        let nsName = baseName as NSString
        let ext = nsName.pathExtension
        let stem = nsName.deletingPathExtension

        // `.overwritePossible` backends — local manifest can't see a peer's
        // pending bytes, so suffix unconditionally.
        if forceWriterIDSuffix, let writerID {
            return resolveWithWriterIDSuffix(stem: stem, ext: ext, writerID: writerID, collisionKeys: collisionKeys)
        }

        guard collisionKeys.contains(collisionKey(for: baseName)) else {
            return baseName
        }

        if let writerID {
            return resolveWithWriterIDSuffix(stem: stem, ext: ext, writerID: writerID, collisionKeys: collisionKeys)
        }

        var suffix = 1
        while true {
            let candidateStem = "\(stem)_\(suffix)"
            let candidate = ext.isEmpty ? candidateStem : "\(candidateStem).\(ext)"
            if !collisionKeys.contains(collisionKey(for: candidate)) {
                return candidate
            }
            suffix += 1
        }
    }

    private static func resolveWithWriterIDSuffix(
        stem: String,
        ext: String,
        writerID: String,
        collisionKeys: Set<String>
    ) -> String {
        let wid6 = RepoLayout.writerIDShort(writerID)
        let basicCandidate = ext.isEmpty ? "\(stem)~\(wid6)" : "\(stem)~\(wid6).\(ext)"
        if !collisionKeys.contains(collisionKey(for: basicCandidate)) {
            return basicCandidate
        }
        // Hard upper bound — a poisoned manifest or hostile remote could otherwise
        // make this loop chew CPU + heap chasing a never-free suffix.
        let maxSuffix = 10_000
        var suffix = 1
        while suffix <= maxSuffix {
            let candidateStem = "\(stem)~\(wid6)-\(suffix)"
            let candidate = ext.isEmpty ? candidateStem : "\(candidateStem).\(ext)"
            if !collisionKeys.contains(collisionKey(for: candidate)) {
                return candidate
            }
            suffix += 1
        }
        // Fall back to UUID-stamped name — guaranteed unique, breaks the loop. Caller
        // will see an unfamiliar name and we surface via assertion in debug.
        assertionFailure("RemoteFileNaming: writer suffix exhausted for stem=\(stem) wid=\(wid6); collision set size=\(collisionKeys.count)")
        let escape = UUID().uuidString.lowercased().prefix(8)
        let escapeStem = "\(stem)~\(wid6)-\(escape)"
        return ext.isEmpty ? escapeStem : "\(escapeStem).\(ext)"
    }

    static func collisionKeySet(from fileNames: Set<String>) -> Set<String> {
        Set(fileNames.map(collisionKey(for:)))
    }

    /// Canonical writerID-suffixed name (no numeric tiebreaker); used by orphan-reuse probe.
    static func writerIDSuffixedName(baseName: String, writerID: String) -> String {
        let nsName = baseName as NSString
        let ext = nsName.pathExtension
        let stem = nsName.deletingPathExtension
        let wid6 = RepoLayout.writerIDShort(writerID)
        return ext.isEmpty ? "\(stem)~\(wid6)" : "\(stem)~\(wid6).\(ext)"
    }

    static func collisionKey(for fileName: String) -> String {
        fileName.precomposedStringWithCanonicalMapping
            .folding(options: [.caseInsensitive, .diacriticInsensitive], locale: Locale(identifier: "en_US_POSIX"))
    }

    private static func resourceTypeRawName(forRole role: Int) -> String {
        switch role {
        case ResourceTypeCode.photo: return "photo"
        case ResourceTypeCode.video: return "video"
        case ResourceTypeCode.audio: return "audio"
        case ResourceTypeCode.alternatePhoto: return "alternatePhoto"
        case ResourceTypeCode.fullSizePhoto: return "fullSizePhoto"
        case ResourceTypeCode.fullSizeVideo: return "fullSizeVideo"
        case ResourceTypeCode.pairedVideo: return "pairedVideo"
        case ResourceTypeCode.adjustmentData: return "adjustmentData"
        case ResourceTypeCode.adjustmentBasePhoto: return "adjustmentBasePhoto"
        case ResourceTypeCode.fullSizePairedVideo: return "fullSizePairedVideo"
        case ResourceTypeCode.adjustmentBasePairedVideo: return "adjustmentBasePairedVideo"
        case ResourceTypeCode.adjustmentBaseVideo: return "adjustmentBaseVideo"
        case ResourceTypeCode.photoProxy: return "photoProxy"
        default: return "unknown"
        }
    }
}
