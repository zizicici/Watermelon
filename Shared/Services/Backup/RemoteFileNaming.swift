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

    static func resolveNextAvailableName(baseName: String, occupiedNames: Set<String>) -> String {
        resolveNextAvailableName(baseName: baseName, collisionKeys: collisionKeySet(from: occupiedNames))
    }

    static func resolveNextAvailableName(baseName: String, collisionKeys: Set<String>) -> String {
        guard collisionKeys.contains(collisionKey(for: baseName)) else {
            return baseName
        }

        let nsName = baseName as NSString
        let ext = nsName.pathExtension
        let stem = nsName.deletingPathExtension

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

    static func collisionKeySet(from fileNames: Set<String>) -> Set<String> {
        Set(fileNames.map(collisionKey(for:)))
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
