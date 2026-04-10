# Home Page Refactoring - Review Document

## Project Background

iOS photo backup app ("Watermelon"). The home page (`NewHomeViewController`) displays a two-column layout:
- **Left column**: Local photo library months (from `PHAsset`)
- **Right column**: Remote storage months (from backup manifests)

Users select months on either/both sides, then execute upload/download/sync operations. The page needs to update in real-time during these operations (progress, spinner animations, completion state).

## Architecture Overview

### Data Layer

- **`HomeIncrementalDataManager`** (`@MainActor`): Owns three internal engines:
  - `HomeLocalIndexEngine` — in-memory index of all `PHAsset` items grouped by month
  - `HomeRemoteIndexEngine` — in-memory index of remote snapshot data
  - `HomeReconcileEngine` — merges local and remote per-month into `HomeAlbumItem` with `.localOnly` / `.remoteOnly` / `.both` tags
- Exposes: `localMonthSummaries()`, `matchedCount(for:)`, `remoteOnlyItems(for:)`, `localAssetIDs(for:)`
- Callbacks: `onMonthsChanged: ((Set<LibraryMonthKey>) -> Void)?` (photo library changes), `onFileSizesUpdated: ((Set<LibraryMonthKey>) -> Void)?`

### Execution Layer

- **`HomeExecutionCoordinator`** (`@MainActor`): Manages the three-phase execution flow:
  1. **Upload phase**: `BackupSessionController` drives `BackupCoordinator.runBackup` for selected local months
  2. **Download phase**: Sequential per-month — scoped backup (to populate hash index) then `RestoreService.restoreItems`
  3. Sync months participate in both phases (upload first, then download)

### View Layer

- **`NewHomeViewController`**: UICollectionView with `UICollectionViewCompositionalLayout` (two items per group = two columns), `UICollectionViewDiffableDataSource`, section headers per year, direction arrow badges between columns
- **`SelectionActionPanel`**: Bottom bar showing selection counts and execution controls (pause/resume/stop/complete)

## Changes Made

### Change 1: Execution State Machine — `MonthPlan`

**Problem**: The coordinator used flat sets (`executionMonths`, `completedMonths`, `activeMonths`) to track execution state. When a sync month's manifest was flushed during the upload phase, it was added to `completedMonths` via `snapshot.flushedMonths`. Since `configureCell` checked `completedMonths` before `activeMonths`, the sync month showed a green completion checkmark during the download phase instead of a running spinner. The download happened silently in the background with no visual feedback.

**Solution**: Replace flat sets with a per-month plan:

```swift
private struct MonthPlan {
    let needsUpload: Bool
    let needsDownload: Bool
    var uploadDone = false
    var downloadDone = false

    var isFullyCompleted: Bool {
        (!needsUpload || uploadDone) && (!needsDownload || downloadDone)
    }
}

private var monthPlans: [LibraryMonthKey: MonthPlan] = [:]
```

Built during `enter()`:
- Upload months: `MonthPlan(needsUpload: true, needsDownload: false)`
- Download months: `MonthPlan(needsUpload: false, needsDownload: true)`
- Sync months: `MonthPlan(needsUpload: true, needsDownload: true)`

When a month flushes during upload: `monthPlans[month]?.uploadDone = true`. For upload-only months, `isFullyCompleted` becomes `true`. For sync months, it remains `false` — the cell continues showing the spinner.

When a month's download completes: `monthPlans[month]?.downloadDone = true`. Now `isFullyCompleted` becomes `true`.

The public `MonthProgress` struct derives `executionMonths` and `completedMonths` from `monthPlans`:
```swift
func monthProgress() -> MonthProgress {
    MonthProgress(
        executionMonths: Set(monthPlans.keys),
        completedMonths: Set(monthPlans.filter { $0.value.isFullyCompleted }.map(\.key)),
        activeMonths: activeMonths
    )
}
```

**Also fixed**: Download phase resume now filters already-completed months:
```swift
let remainingDownloads = pendingDownloadMonths.filter { monthPlans[$0]?.isFullyCompleted != true }
```

### Change 2: Unified Update Path

**Problem**: Two separate update methods existed:
- `rebuildSnapshot()` — full structural rebuild, refreshed `mergedSections` and `rowLookup` from live data
- `invalidateMonths(_:)` — content-only reconfigure, did NOT refresh `rowLookup`

This meant cells, year headers, and arrow badges read stale cached data during `invalidateMonths` calls.

**Solution**: Single entry point `updateUI(changedMonths:refreshData:)`:

```swift
private func updateUI(changedMonths: Set<LibraryMonthKey>? = nil, refreshData: Bool = true) {
    // 1. Optionally refresh mergedSections + rowLookup from live data
    if refreshData {
        refreshSectionsAndRowLookup()
    }

    // 2. Connection state checks (overlay, selection guard)
    // ...

    // 3. Structural vs content-only UI update
    if changedMonths == nil || monthSetChanged {
        applyFullSnapshot()       // full NSDiffableDataSourceSnapshot rebuild
    } else {
        reconfigureMonths(...)    // targeted reconfigureItems + visible headers/arrows
    }
}
```

**`refreshData` parameter rationale**: 

During execution progress ticks, the summary data (photo/video counts, file sizes) has not changed — only the execution state (active/completed months, processed counts) changes. `refreshSectionsAndRowLookup()` calls `localMonthBackedUpCounts()` and `localMonthMediaCounts()` which iterate all ~100K assets. Calling this on every progress tick would cause a performance regression.

Therefore:
- `onMonthsChanged` (photo library changes) → `refreshData: true` — actual data changed
- `onFileSizesUpdated` → `refreshData: true` — actual data changed
- Coordinator content-only progress updates → `refreshData: false` — only execution state changed
- Structural changes (connection, phase transitions) → `refreshData: true` (default)

**Structural change auto-detection**: When `refreshData: true` and `changedMonths` is non-nil, the method compares `previousMonths` vs `currentMonths` (keys of `rowLookup`). If the set of months changed (e.g., a month's photos were all deleted), it automatically upgrades to a full snapshot rebuild.

### Change 3: Coordinator Callback Merge

**Problem**: Two separate callbacks `onInvalidateMonths` and `onRebuildSnapshot` mapped 1:1 to the two update paths, coupling the coordinator to the VC's internal update mechanism.

**Solution**: Single callback `onUIUpdate: ((Set<LibraryMonthKey>?) -> Void)?`:
- `nil` → structural update (phase transition, failure)
- Non-nil → content-only update for specific months (progress tick)

VC binding:
```swift
executionCoordinator.onUIUpdate = { [weak self] changedMonths in
    if let months = changedMonths {
        self?.updateUI(changedMonths: months, refreshData: false)
    } else {
        self?.updateUI()
    }
}
```

### Change 4: Right-Side Overlay for Connection States

**Problem**: When no remote storage is connected, the right column shows empty cells with zero counts, which is confusing.

**Solution**: A `UIView` overlay covering the right half of the collection view area, positioned from below the right header to above the action panel. Three states:

| State | Overlay Content |
|---|---|
| `isConnecting == true` | Spinner + "连接中..." |
| `!hasActiveConnection` | "未连接远端存储" + profile selection button (opens same menu as right header) |
| Connected | Hidden |

The overlay is updated via `updateRemoteOverlay()` called from `updateUI()` and directly when `isConnecting` changes in `connect()`.

### Change 5: Selection Guard

**Problem**: Users could select months and attempt operations when no remote storage is connected, which is meaningless.

**Solution**:

1. **Toggle disable**: `updateSelectionInteraction()` sets `leftToggle.isEnabled` and `rightToggle.isEnabled` based on `hasActiveConnection && !executionCoordinator.isActive`. Profile menu buttons are disabled only during execution (not when disconnected — user needs them to connect).

2. **Selection clearing**: When `!hasActiveConnection`, `updateUI` clears `selectedLocalMonths` and `selectedRemoteMonths`. This is intentionally NOT triggered by `executionCoordinator.isActive` — selections must be preserved during execution because `arrowDirection(for:)` reads from them to determine upload/download/sync direction for progress display.

3. **Tap guard**: `didSelectItemAt` and year header tap closures check `hasActiveConnection && !executionCoordinator.isActive`.

4. **Execution end**: `onExecutionEnded` explicitly clears selections (not relying on `updateUI` since the connection is still active).

### Change 6: Thread Safety Fix — `startFileSizeScan`

**Problem**: `HomeIncrementalDataManager.startFileSizeScan()` used `Task.detached` which escapes the `@MainActor` context. The captured `localIndex` (a non-Sendable class) was read from a background thread while the main thread could mutate it via `reloadAll`/`applyPhotoLibraryChange`. This is a data race.

**Solution**: Changed `Task.detached` to `Task`, which inherits the `@MainActor` context. Added `await Task.yield()` between months to avoid blocking the main thread. `PHAssetResource.assetResources(for:)` is a synchronous metadata lookup (~1-10μs per call), so running on the main thread with periodic yielding is acceptable.

## Key Invariants

1. **`rowLookup` freshness**: Always refreshed from live data when `refreshData: true`. Guaranteed fresh for structural changes. May be stale for execution progress ticks (acceptable — summary data doesn't change during progress).

2. **Selection preservation during execution**: `updateUI` only clears selections when `!hasActiveConnection`, never due to `isActive`. Selections are explicitly cleared in `onExecutionEnded`.

3. **MonthPlan completion semantics**: `isFullyCompleted` is true only when ALL required phases are done. Upload-only months complete on upload flush. Sync months require both upload AND download completion.

4. **Content-only updates never cause full collection view reload**: `reconfigureMonths()` uses `reconfigureItems` (targeted) — only affects specified cells. `applyFullSnapshot()` is only called for structural changes.

5. **Overlay covers right column only**: Positioned from `view.centerX + 1` to `trailing`, ensuring left column (local data) is always visible regardless of connection state.

## Files Modified

| File | Changes |
|---|---|
| `HomeExecutionCoordinator.swift` | `MonthPlan` struct, replace flat sets, callback merge, download resume fix |
| `NewHomeViewController.swift` | Unified `updateUI`, overlay view, selection guard, binding updates |
| `HomeLibraryEngines.swift` | `Task.detached` → `Task`, callbacks return `Set<LibraryMonthKey>` instead of `Bool`/`Void` |
