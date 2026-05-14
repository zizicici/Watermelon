import CryptoKit
import Foundation

enum RemoteFileNaming {
    private static let emergencyStableTokenOccupiedKeyLimit = 256

    enum ResolutionError: LocalizedError {
        case exhausted(stem: String, collisionCount: Int)

        var errorDescription: String? {
            switch self {
            case .exhausted(let stem, let collisionCount):
                return "Unable to find a collision-free remote filename for \(stem) after checking \(collisionCount) existing names"
            }
        }
    }

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
        let availableBytes = maxLeafByteBudget - max(reservedSuffixBytes, 0)
        guard availableBytes > 0 else { return "" }
        let extPortion = ext.isEmpty ? "" : "." + ext
        let extMaxBytes = extPortion.isEmpty ? 0 : min(maxLeafByteBudget / 2, max(availableBytes - 1, 0))
        let extBudget = extMaxBytes > 0 ? clampStringToBytes(extPortion, maxBytes: extMaxBytes) : ""
        let remaining = max(availableBytes - extBudget.utf8.count, 1)
        let clampedStem = clampStringToBytes(stem, maxBytes: remaining)
        return clampedStem + extBudget
    }

    static func clampStringToBytes(_ value: String, maxBytes: Int) -> String {
        guard maxBytes > 0 else { return "_" }
        if value.utf8.count <= maxBytes { return value }
        var result = ""
        result.reserveCapacity(min(value.count, maxBytes))
        var byteCount = 0
        for scalar in value.unicodeScalars {
            let scalarByteCount = String(scalar).utf8.count
            guard byteCount + scalarByteCount <= maxBytes else { break }
            result.unicodeScalars.append(scalar)
            byteCount += scalarByteCount
        }
        return result.isEmpty ? "_" : result
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
        caseSensitivity: BackendNameCaseSensitivity = .caseInsensitive,
        writerID: String? = nil,
        forceWriterIDSuffix: Bool = false
    ) -> String {
        do {
            return try resolveNextAvailableNameOrThrow(
                baseName: baseName,
                occupiedNames: occupiedNames,
                caseSensitivity: caseSensitivity,
                writerID: writerID,
                forceWriterIDSuffix: forceWriterIDSuffix
            )
        } catch {
            assertionFailure(error.localizedDescription)
            return emergencyFallbackName(
                baseName: baseName,
                writerID: writerID,
                forceWriterIDSuffix: forceWriterIDSuffix,
                occupiedKeys: nameKeySet(from: occupiedNames, caseSensitivity: caseSensitivity),
                caseSensitivity: caseSensitivity
            )
        }
    }

    static func resolveNextAvailableNameOrThrow(
        baseName: String,
        occupiedNames: Set<String>,
        caseSensitivity: BackendNameCaseSensitivity = .caseInsensitive,
        writerID: String? = nil,
        forceWriterIDSuffix: Bool = false
    ) throws -> String {
        try resolveNextAvailableNameOrThrow(
            baseName: baseName,
            occupiedKeys: nameKeySet(from: occupiedNames, caseSensitivity: caseSensitivity),
            caseSensitivity: caseSensitivity,
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
        do {
            return try resolveNextAvailableNameOrThrow(
                baseName: baseName,
                collisionKeys: collisionKeys,
                writerID: writerID,
                forceWriterIDSuffix: forceWriterIDSuffix
            )
        } catch {
            assertionFailure(error.localizedDescription)
            return emergencyFallbackName(
                baseName: baseName,
                writerID: writerID,
                forceWriterIDSuffix: forceWriterIDSuffix,
                occupiedKeys: collisionKeys,
                caseSensitivity: .caseInsensitive
            )
        }
    }

    static func resolveNextAvailableNameOrThrow(
        baseName: String,
        collisionKeys: Set<String>,
        writerID: String? = nil,
        forceWriterIDSuffix: Bool = false
    ) throws -> String {
        try resolveNextAvailableNameOrThrow(
            baseName: baseName,
            occupiedKeys: collisionKeys,
            caseSensitivity: .caseInsensitive,
            writerID: writerID,
            forceWriterIDSuffix: forceWriterIDSuffix
        )
    }

    static func resolveNextAvailableNameOrThrow(
        baseName: String,
        occupiedKeys: Set<String>,
        caseSensitivity: BackendNameCaseSensitivity,
        writerID: String? = nil,
        forceWriterIDSuffix: Bool = false
    ) throws -> String {
        let nsName = baseName as NSString
        let ext = nsName.pathExtension
        let stem = nsName.deletingPathExtension

        if forceWriterIDSuffix, let writerID {
            return try resolveWithWriterIDSuffix(
                stem: stem,
                ext: ext,
                writerID: writerID,
                occupiedKeys: occupiedKeys,
                caseSensitivity: caseSensitivity
            )
        }

        guard occupiedKeys.contains(nameKey(for: baseName, caseSensitivity: caseSensitivity)) else {
            return baseName
        }

        if let writerID {
            return try resolveWithWriterIDSuffix(
                stem: stem,
                ext: ext,
                writerID: writerID,
                occupiedKeys: occupiedKeys,
                caseSensitivity: caseSensitivity
            )
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
            if !occupiedKeys.contains(nameKey(for: candidate, caseSensitivity: caseSensitivity)) {
                return candidate
            }
            suffix += 1
        }
        // Every other branch returns a verified-unique name; the UUID fallback must honor that invariant.
        for _ in 0..<32 {
            let escape = String(UUID().uuidString.lowercased().prefix(8))
            let stemBudget = max(maxLeafByteBudget - escape.utf8.count - 1 - extBudget.utf8.count, 1)
            let candidate = clampStringToBytes(stem, maxBytes: stemBudget) + "_" + escape + extBudget
            if !occupiedKeys.contains(nameKey(for: candidate, caseSensitivity: caseSensitivity)) {
                return candidate
            }
        }
        if let widened = widenedUUIDFallback(
            stem: stem,
            ext: ext,
            extBudget: extBudget,
            occupiedKeys: occupiedKeys,
            caseSensitivity: caseSensitivity
        ) {
            return widened
        }
        throw ResolutionError.exhausted(stem: stem, collisionCount: occupiedKeys.count)
    }

    private static func resolveWithWriterIDSuffix(
        stem: String,
        ext: String,
        writerID: String,
        occupiedKeys: Set<String>,
        caseSensitivity: BackendNameCaseSensitivity
    ) throws -> String {
        let basicCandidate = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID)
        if !occupiedKeys.contains(nameKey(for: basicCandidate, caseSensitivity: caseSensitivity)) {
            return basicCandidate
        }
        let maxSuffix = 10_000
        var suffix = 1
        while suffix <= maxSuffix {
            let candidate = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, numericSuffix: suffix)
            if !occupiedKeys.contains(nameKey(for: candidate, caseSensitivity: caseSensitivity)) {
                return candidate
            }
            suffix += 1
        }
        // Final fallback must also be verified — short escape token still has a non-zero collision rate.
        for _ in 0..<32 {
            let escape = String(UUID().uuidString.lowercased().prefix(8))
            let candidate = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, escapeToken: escape)
            if !occupiedKeys.contains(nameKey(for: candidate, caseSensitivity: caseSensitivity)) {
                return candidate
            }
        }
        let fullEscape = UUID().uuidString.lowercased().replacingOccurrences(of: "-", with: "")
        let widened = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, escapeToken: fullEscape)
        if !occupiedKeys.contains(nameKey(for: widened, caseSensitivity: caseSensitivity)) {
            return widened
        }
        throw ResolutionError.exhausted(stem: stem, collisionCount: occupiedKeys.count)
    }

    /// Last-resort: full 32-hex UUID makes collision astronomically improbable; still verified before return.
    private static func widenedUUIDFallback(
        stem: String,
        ext: String,
        extBudget: String,
        occupiedKeys: Set<String>,
        caseSensitivity: BackendNameCaseSensitivity
    ) -> String? {
        for _ in 0..<8 {
            let escape = UUID().uuidString.lowercased().replacingOccurrences(of: "-", with: "")
            let stemBudget = max(maxLeafByteBudget - escape.utf8.count - 1 - extBudget.utf8.count, 1)
            let candidate = clampStringToBytes(stem, maxBytes: stemBudget) + "_" + escape + extBudget
            if !occupiedKeys.contains(nameKey(for: candidate, caseSensitivity: caseSensitivity)) {
                return candidate
            }
        }
        return nil
    }

    private static func emergencyFallbackName(
        baseName: String,
        writerID: String?,
        forceWriterIDSuffix: Bool,
        occupiedKeys: Set<String>,
        caseSensitivity: BackendNameCaseSensitivity
    ) -> String {
        let nsName = baseName as NSString
        let ext = nsName.pathExtension
        let stem = nsName.deletingPathExtension
        let extPortion = ext.isEmpty ? "" : "." + ext
        let extBudget = clampStringToBytes(extPortion, maxBytes: maxLeafByteBudget / 2)
        for _ in 0..<64 {
            let token = UUID().uuidString.lowercased().replacingOccurrences(of: "-", with: "")
            let candidate: String
            if forceWriterIDSuffix, let writerID {
                candidate = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, escapeToken: token)
            } else {
                let stemBudget = max(maxLeafByteBudget - token.utf8.count - 1 - extBudget.utf8.count, 1)
                candidate = clampStringToBytes(stem, maxBytes: stemBudget) + "_" + token + extBudget
            }
            if !occupiedKeys.contains(nameKey(for: candidate, caseSensitivity: caseSensitivity)) {
                return candidate
            }
        }
        let stableToken = emergencyStableToken(
            baseName: baseName,
            writerID: writerID,
            forceWriterIDSuffix: forceWriterIDSuffix,
            occupiedKeys: occupiedKeys
        )
        for suffix in 0..<4096 {
            let token = suffix == 0 ? stableToken : "\(stableToken)-\(String(suffix, radix: 36))"
            let candidate: String
            if forceWriterIDSuffix, let writerID {
                candidate = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, escapeToken: token)
            } else {
                let stemBudget = max(maxLeafByteBudget - token.utf8.count - 1 - extBudget.utf8.count, 1)
                candidate = clampStringToBytes(stem, maxBytes: stemBudget) + "_" + token + extBudget
            }
            if !occupiedKeys.contains(nameKey(for: candidate, caseSensitivity: caseSensitivity)) {
                return candidate
            }
        }
        let extendedSuffixLimit = 4096 + 256
        for suffix in 4096..<extendedSuffixLimit {
            let token = "\(stableToken)-\(String(suffix, radix: 36))"
            let candidate: String
            if forceWriterIDSuffix, let writerID {
                candidate = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, escapeToken: token)
            } else {
                let stemBudget = max(maxLeafByteBudget - token.utf8.count - 1 - extBudget.utf8.count, 1)
                candidate = clampStringToBytes(stem, maxBytes: stemBudget) + "_" + token + extBudget
            }
            if !occupiedKeys.contains(nameKey(for: candidate, caseSensitivity: caseSensitivity)) {
                return candidate
            }
        }
        let finalSuffixLimit = extendedSuffixLimit + 10_000
        for suffix in extendedSuffixLimit..<finalSuffixLimit {
            let token = "\(stableToken)-\(String(suffix, radix: 36))"
            let candidate: String
            if forceWriterIDSuffix, let writerID {
                candidate = writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, escapeToken: token)
            } else {
                let stemBudget = max(maxLeafByteBudget - token.utf8.count - 1 - extBudget.utf8.count, 1)
                candidate = clampStringToBytes(stem, maxBytes: stemBudget) + "_" + token + extBudget
            }
            if !occupiedKeys.contains(nameKey(for: candidate, caseSensitivity: caseSensitivity)) {
                return candidate
            }
        }
        let compactStableToken = String(stableToken.prefix(16))
        for discriminator in 0...occupiedKeys.count {
            let token = "\(String(discriminator, radix: 36))-\(compactStableToken)"
            let candidate: String
            if forceWriterIDSuffix, let writerID {
                candidate = writerIDSuffixedName(stem: "wm", ext: ext, writerID: writerID, escapeToken: token)
            } else {
                candidate = "wm_\(token)" + extBudget
            }
            if !occupiedKeys.contains(nameKey(for: candidate, caseSensitivity: caseSensitivity)) {
                return candidate
            }
        }
        preconditionFailure("finite occupied set must leave at least one compact fallback name")
    }

    private static func emergencyStableToken(
        baseName: String,
        writerID: String?,
        forceWriterIDSuffix: Bool,
        occupiedKeys: Set<String>
    ) -> String {
        var hasher = SHA256()
        for part in [baseName, writerID ?? "", forceWriterIDSuffix ? "1" : "0"] {
            hasher.update(data: Data(part.utf8))
            hasher.update(data: Data("\n".utf8))
        }
        hasher.update(data: Data("occupied_count=\(occupiedKeys.count)\n".utf8))
        for key in emergencyStableTokenOccupiedKeys(occupiedKeys) {
            hasher.update(data: Data(key.utf8))
            hasher.update(data: Data("\n".utf8))
        }
        return Data(hasher.finalize()).hexString
    }

    private static func emergencyStableTokenOccupiedKeys(_ occupiedKeys: Set<String>) -> [String] {
        let limit = min(occupiedKeys.count, emergencyStableTokenOccupiedKeyLimit)
        guard limit > 0 else { return [] }
        var sample: [(digest: String, key: String)] = []
        sample.reserveCapacity(limit)
        for key in occupiedKeys {
            let digest = Data(SHA256.hash(data: Data(key.utf8))).hexString
            let entry = (digest: digest, key: key)
            if sample.count < limit {
                sample.append(entry)
                if sample.count == limit {
                    sample.sort(by: emergencyStableTokenSamplePrecedes)
                }
            } else if emergencyStableTokenSamplePrecedes(entry, sample[limit - 1]) {
                sample[limit - 1] = entry
                sample.sort(by: emergencyStableTokenSamplePrecedes)
            }
        }
        if sample.count < limit {
            sample.sort(by: emergencyStableTokenSamplePrecedes)
        }
        return sample.map(\.key)
    }

    private static func emergencyStableTokenSamplePrecedes(
        _ lhs: (digest: String, key: String),
        _ rhs: (digest: String, key: String)
    ) -> Bool {
        if lhs.digest != rhs.digest { return lhs.digest < rhs.digest }
        return lhs.key < rhs.key
    }

    static func collisionKeySet(from fileNames: Set<String>) -> Set<String> {
        Set(fileNames.map(collisionKey(for:)))
    }

    static func nameKey(for fileName: String, caseSensitivity: BackendNameCaseSensitivity) -> String {
        switch caseSensitivity {
        case .caseSensitive: return fileName
        case .caseInsensitive: return collisionKey(for: fileName)
        }
    }

    static func nameKeySet(from fileNames: Set<String>, caseSensitivity: BackendNameCaseSensitivity) -> Set<String> {
        Set(fileNames.map { nameKey(for: $0, caseSensitivity: caseSensitivity) })
    }

    /// Canonical writerID-suffixed name (no numeric tiebreaker); used by orphan-reuse probe.
    static func writerIDSuffixedName(baseName: String, writerID: String) -> String {
        let nsName = baseName as NSString
        let ext = nsName.pathExtension
        let stem = nsName.deletingPathExtension
        return writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID)
    }

    /// Writer + run suffix so a same-writer prior-run multipart orphan can't be clobbered by this run's multipart completion.
    static func writerIDRunIDSuffixedName(baseName: String, writerID: String, runID: String) -> String {
        let nsName = baseName as NSString
        let ext = nsName.pathExtension
        let stem = nsName.deletingPathExtension
        return writerIDSuffixedName(stem: stem, ext: ext, writerID: writerID, runIDToken: RepoLayout.runIDPrefix(runID))
    }

    private static func writerIDSuffixedName(
        stem: String,
        ext: String,
        writerID: String,
        numericSuffix: Int? = nil,
        escapeToken: String? = nil,
        runIDToken: String? = nil
    ) -> String {
        let wid6 = RepoLayout.writerIDShort(writerID)
        var trailing = "~\(wid6)"
        if let runIDToken, !runIDToken.isEmpty {
            trailing += "-r\(runIDToken)"
        }
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
