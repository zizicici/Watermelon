import Foundation

struct MediaBrowserSelectionSummary: Equatable {
    private(set) var count = 0
    private(set) var photoCount = 0
    private(set) var videoCount = 0
    private(set) var deviceCount = 0
    private(set) var remoteCount = 0
    private var localOnlyCount = 0
    private var remoteOnlyCount = 0

    var actions: BatchActionResolver.Result {
        .init(showsUpload: count > 0 && localOnlyCount == count,
              showsDownload: count > 0 && remoteOnlyCount == count,
              deviceCount: deviceCount, remoteCount: remoteCount)
    }

    mutating func include(_ item: MediaBrowserItem, selected: Bool) {
        let delta = selected ? 1 : -1
        count += delta
        if item.isVideo { videoCount += delta } else { photoCount += delta }
        if item.isDeviceDeletable { deviceCount += delta }
        if item.isRemoteDeletable { remoteCount += delta }
        if item.presence == .localOnly { localOnlyCount += delta }
        if item.presence == .remoteOnly { remoteOnlyCount += delta }
    }
}

@MainActor
final class MediaBrowserSelection {
    private(set) var snapshot = MediaBrowserSnapshot.empty
    private(set) var isSelecting = false
    private(set) var selectedIDs: Set<MediaBrowserItemID> = []
    private(set) var summary = MediaBrowserSelectionSummary()
    private(set) var revision: UInt64 = 0
    private var selectedCountsBySection: [Int: Int] = [:]
    private var orderedItemsCache: [MediaBrowserItem]?
    private var drag: Drag?

    private struct Drag {
        let anchor: Int
        let selects: Bool
        var range: ClosedRange<Int>?
        var originalSelection: [MediaBrowserItemID: Bool] = [:]
    }

    var isDragging: Bool { drag != nil }

    var orderedItems: [MediaBrowserItem] {
        if let orderedItemsCache { return orderedItemsCache }
        let items = snapshot.items(inDisplayOrderFor: selectedIDs)
        orderedItemsCache = items
        return items
    }

    func replaceSnapshot(_ snapshot: MediaBrowserSnapshot) {
        endDrag()
        self.snapshot = snapshot
        selectedIDs = selectedIDs.filter { snapshot.index(of: $0) != nil }
        rebuildSummary()
        changed()
    }

    @discardableResult
    func enter() -> Bool {
        guard !isSelecting else { return false }
        isSelecting = true
        changed()
        return true
    }

    func exit() {
        endDrag()
        isSelecting = false
        clear()
    }

    func clear() {
        endDrag()
        selectedIDs.removeAll()
        selectedCountsBySection.removeAll()
        summary = MediaBrowserSelectionSummary()
        changed()
    }

    @discardableResult
    func selectAll() -> Bool {
        guard isSelecting, !isDragging, selectedIDs.count != snapshot.itemCount else { return false }
        selectedIDs = Set(snapshot.itemIDs)
        rebuildSummary()
        changed()
        return true
    }

    func isSectionSelected(_ sectionIndex: Int) -> Bool {
        let count = snapshot.itemCount(inSection: sectionIndex)
        return count > 0 && selectedCountsBySection[sectionIndex] == count
    }

    @discardableResult
    func toggleSection(_ sectionIndex: Int) -> Bool {
        guard isSelecting, !isDragging else { return false }
        let ids = snapshot.itemIDs(inSection: sectionIndex)
        guard !ids.isEmpty else { return false }
        let selects = !isSectionSelected(sectionIndex)
        for id in ids { _ = setMembership(selects, for: id) }
        changed()
        return true
    }

    @discardableResult
    func setSelected(_ selected: Bool, for id: MediaBrowserItemID) -> Bool {
        guard isSelecting, !isDragging, setMembership(selected, for: id) else { return false }
        changed()
        return true
    }

    @discardableResult
    func remove(_ ids: some Sequence<MediaBrowserItemID>) -> Bool {
        guard isSelecting, !isDragging else { return false }
        var didChange = false
        for id in ids {
            if setMembership(false, for: id) { didChange = true }
        }
        if didChange { changed() }
        return didChange
    }

    @discardableResult
    func beginDrag(at id: MediaBrowserItemID) -> Bool {
        guard isSelecting, !isDragging, let anchor = snapshot.index(of: id) else { return false }
        drag = Drag(anchor: anchor, selects: !selectedIDs.contains(id))
        return true
    }

    @discardableResult
    func updateDrag(through id: MediaBrowserItemID) -> Bool {
        guard var drag, let index = snapshot.index(of: id) else { return false }
        let range = min(drag.anchor, index)...max(drag.anchor, index)
        guard range != drag.range else { return false }
        self.drag = nil
        var didChange = false
        if let previous = drag.range {
            for portion in Self.subtract(range, from: previous) {
                for index in portion {
                    guard let item = snapshot.item(at: index),
                          let original = drag.originalSelection[item.id] else { continue }
                    if setMembership(original, for: item.id) { didChange = true }
                }
            }
        }
        for portion in Self.subtract(drag.range, from: range) {
            for index in portion {
                guard let item = snapshot.item(at: index) else { continue }
                if drag.originalSelection[item.id] == nil {
                    drag.originalSelection[item.id] = selectedIDs.contains(item.id)
                }
                if setMembership(drag.selects, for: item.id) { didChange = true }
            }
        }
        drag.range = range
        self.drag = drag
        if didChange { changed() }
        return didChange
    }

    func endDrag() { drag = nil }

    private func setMembership(_ selected: Bool, for id: MediaBrowserItemID) -> Bool {
        guard selectedIDs.contains(id) != selected, let item = snapshot.item(id: id) else { return false }
        if selected { selectedIDs.insert(id) } else { selectedIDs.remove(id) }
        summary.include(item, selected: selected)
        if let sectionIndex = snapshot.sectionIndex(of: id) {
            selectedCountsBySection[sectionIndex, default: 0] += selected ? 1 : -1
        }
        return true
    }

    private func rebuildSummary() {
        summary = MediaBrowserSelectionSummary()
        selectedCountsBySection.removeAll(keepingCapacity: true)
        for id in selectedIDs {
            if let item = snapshot.item(id: id) { summary.include(item, selected: true) }
            if let sectionIndex = snapshot.sectionIndex(of: id) {
                selectedCountsBySection[sectionIndex, default: 0] += 1
            }
        }
    }

    private func changed() {
        revision &+= 1
        orderedItemsCache = nil
    }

    private static func subtract(_ excluded: ClosedRange<Int>?, from range: ClosedRange<Int>) -> [ClosedRange<Int>] {
        guard let excluded, range.overlaps(excluded) else { return [range] }
        var portions: [ClosedRange<Int>] = []
        if range.lowerBound < excluded.lowerBound { portions.append(range.lowerBound...(excluded.lowerBound - 1)) }
        if range.upperBound > excluded.upperBound { portions.append((excluded.upperBound + 1)...range.upperBound) }
        return portions
    }
}
