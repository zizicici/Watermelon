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

        return composeLeaf(stem: stem, ext: originalExt, reservedSuffixBytes: 0)
    }

    static let maxLeafByteBudget = 255
    static let writerIDSuffixReserveBytes = 16

    static func composeLeaf(stem: String, ext: String, reservedSuffixBytes: Int) -> String {
        let extPortion = ext.isEmpty ? "" : "." + ext
        let extBudget = clampStringToBytes(extPortion, maxBytes: maxLeafByteBudget / 2)
        let remaining = max(maxLeafByteBudget - reservedSuffixBytes - extBudget.utf8.count, 1)
        let clampedStem = clampStringToBytes(stem, maxBytes: remaining)
        return clampedStem + extBudget
    }

    static func clampStringToBytes(_ value: String, maxBytes: Int) -> String {
        if value.utf8.count <= maxBytes { return value }
        var result = value
        while result.utf8.count > maxBytes && !result.isEmpty {
            result.removeLast()
        }
        return result
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

        if forceWriterIDSuffix, let writerID {
            return resolveWithWriterIDSuffix(stem: stem, ext: ext, writerID: writerID, collisionKeys: collisionKeys)
        }

        guard collisionKeys.contains(collisionKey(for: baseName)) else {
            return baseName
        }

        if let writerID {
            return resolveWithWriterIDSuffix(stem: stem, ext: ext, writerID: writerID, collisionKeys: collisionKeys)
        }

        let extPortion = ext.isEmpty ? "" : "." + ext
        let extBudget = clampStringToBytes(extPortion, maxBytes: maxLeafByteBudget / 2)
        let maxSuffix = 1_000_000
        var suffix = 1
        while suffix <= maxSuffix {
            let suffixStr = "_\(suffix)"
            // Reserve suffix bytes before clamping; without this, a 255-byte stem swallows `_N` on truncate and loops forever.
            let stemBudget = max(maxLeafByteBudget - suffixStr.utf8.count - extBudget.utf8.count, 1)
            let clampedStem = clampStringToBytes(stem, maxBytes: stemBudget)
            let candidate = clampedStem + suffixStr + extBudget
            if !collisionKeys.contains(collisionKey(for: candidate)) {
                return candidate
            }
            suffix += 1
        }
        // Every other branch returns a verified-unique name; the UUID fallback must honor that invariant.
        for _ in 0..<32 {
            let escape = String(UUID().uuidString.lowercased().prefix(8))
            let stemBudget = max(maxLeafByteBudget - escape.utf8.count - 1 - extBudget.utf8.count, 1)
            let candidate = clampStringToBytes(stem, maxBytes: stemBudget) + "_" + escape + extBudget
            if !collisionKeys.contains(collisionKey(for: candidate)) {
                return candidate
            }
        }
        if let widened = widenedUUIDFallback(stem: stem, ext: ext, extBudget: extBudget, collisionKeys: collisionKeys) {
            return widened
        }
        fatalError("RemoteFileNaming: numeric + UUID fallbacks exhausted for stem=\(stem); collision set size=\(collisionKeys.count)")
    }

    private static func resolveWithWriterIDSuffix(
        stem: String,
        ext: String,
        writerID: String,
        collisionKeys: Set<String>
    ) -> String {
        let basicCandidate = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID)
        if !collisionKeys.contains(collisionKey(for: basicCandidate)) {
            return basicCandidate
        }
        let maxSuffix = 10_000
        var suffix = 1
        while suffix <= maxSuffix {
            let candidate = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, numericSuffix: suffix)
            if !collisionKeys.contains(collisionKey(for: candidate)) {
                return candidate
            }
            suffix += 1
        }
        // Final fallback must also be verified — short escape token still has a non-zero collision rate.
        for _ in 0..<32 {
            let escape = String(UUID().uuidString.lowercased().prefix(8))
            let candidate = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, escapeToken: escape)
            if !collisionKeys.contains(collisionKey(for: candidate)) {
                return candidate
            }
        }
        let fullEscape = UUID().uuidString.lowercased().replacingOccurrences(of: "-", with: "")
        let widened = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, escapeToken: fullEscape)
        if !collisionKeys.contains(collisionKey(for: widened)) {
            return widened
        }
        fatalError("RemoteFileNaming: writer suffix exhausted for stem=\(stem) wid=\(RepoLayout.writerIDShort(writerID)); collision set size=\(collisionKeys.count)")
    }

    /// Last-resort: full 32-hex UUID makes collision astronomically improbable; still verified before return.
    private static func widenedUUIDFallback(
        stem: String,
        ext: String,
        extBudget: String,
        collisionKeys: Set<String>
    ) -> String? {
        for _ in 0..<8 {
            let escape = UUID().uuidString.lowercased().replacingOccurrences(of: "-", with: "")
            let stemBudget = max(maxLeafByteBudget - escape.utf8.count - 1 - extBudget.utf8.count, 1)
            let candidate = clampStringToBytes(stem, maxBytes: stemBudget) + "_" + escape + extBudget
            if !collisionKeys.contains(collisionKey(for: candidate)) {
                return candidate
            }
        }
        return nil
    }

    static func collisionKeySet(from fileNames: Set<String>) -> Set<String> {
        Set(fileNames.map(collisionKey(for:)))
    }

    /// Canonical writerID-suffixed name (no numeric tiebreaker); used by orphan-reuse probe.
    static func writerIDSuffixedName(baseName: String, writerID: String) -> String {
        let nsName = baseName as NSString
        let ext = nsName.pathExtension
        let stem = nsName.deletingPathExtension
        return writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID)
    }

    private static func writerIDSuffixedName(
        stem: String,
        ext: String,
        writerID: String,
        numericSuffix: Int? = nil,
        escapeToken: String? = nil
    ) -> String {
        let wid6 = RepoLayout.writerIDShort(writerID)
        var trailing = "~\(wid6)"
        if let escapeToken {
            trailing += "-\(escapeToken)"
        } else if let numericSuffix {
            trailing += "-\(numericSuffix)"
        }
        let extPortion = ext.isEmpty ? "" : "." + ext
        let extBudget = clampStringToBytes(extPortion, maxBytes: maxLeafByteBudget / 2)
        let reserved = trailing.utf8.count + extBudget.utf8.count
        let stemBudget = max(maxLeafByteBudget - reserved, 1)
        let clampedStem = clampStringToBytes(stem, maxBytes: stemBudget)
        return clampedStem + trailing + extBudget
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
