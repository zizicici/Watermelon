import Foundation

enum RemoteNameCollisionResolver {
    static func resolveNextAvailableName(baseName: String, occupiedNames: Set<String>) -> String {
        guard occupiedNames.contains(baseName) else {
            return baseName
        }

        let nsName = baseName as NSString
        let ext = nsName.pathExtension
        let stem = nsName.deletingPathExtension

        let parsed = splitStemAndSuffix(stem)
        let root = parsed.root
        var suffix = max(parsed.suffix ?? 0, 0)

        while true {
            suffix += 1
            let candidateStem = "\(root)_\(suffix)"
            let candidate: String
            if ext.isEmpty {
                candidate = candidateStem
            } else {
                candidate = "\(candidateStem).\(ext)"
            }
            if !occupiedNames.contains(candidate) {
                return candidate
            }
        }
    }

    private static func splitStemAndSuffix(_ stem: String) -> (root: String, suffix: Int?) {
        guard let underscore = stem.lastIndex(of: "_") else {
            return (stem, nil)
        }
        let suffixStart = stem.index(after: underscore)
        guard suffixStart < stem.endIndex else {
            return (stem, nil)
        }
        let suffixPart = String(stem[suffixStart...])
        guard let suffixValue = Int(suffixPart) else {
            return (stem, nil)
        }
        let root = String(stem[..<underscore])
        guard !root.isEmpty else {
            return (stem, nil)
        }
        return (root, suffixValue)
    }
}
