# Home Page Refactoring - Current Review

## Background

Watermelon’s Home page is a two-column month view:

- left: local photo-library months
- right: remote-storage months

Users select months on either side and then run upload, download, or sync operations. The current refactor has already landed; this document now describes the architecture that exists in code, not an older proposed state.

## Current Architecture

### View Layer

- `HomeViewController`
  - owns collection view, headers, action panel, remote overlay, and More/settings entry
  - renders state from `HomeScreenStore`

### Store Layer

- `HomeScreenStore`
  - owns `HomeIncrementalDataManager`
  - owns `HomeConnectionController`
  - owns `HomeExecutionCoordinator`
  - exposes `sections`, `rowLookup`, `selection`, `connectionState`, `executionState`
  - translates internal changes into `.data / .selection / .execution / .connection / .structural`

### Execution Layer

- `HomeExecutionCoordinator`
  - creates one `HomeExecutionSession`
  - runs local hash-index preflight
  - drives upload via `BackupSessionController`
  - finalizes sync months inline after upload flush
  - runs remaining download months

### Data Layer

- `HomeIncrementalDataManager`
  - local index engine
  - remote index engine
  - reconcile engine
  - deferred photo-library change draining
  - file-size scan scheduling

## What Changed Relative to the Older Refactor Notes

### 1. The VC no longer owns the execution/update wiring

Older notes described callback plumbing directly into `HomeViewController`. The current code routes everything through `HomeScreenStore`, which now acts as the stable UI-facing boundary.

### 2. Execution state is session-based and month-plan based

The current state model lives in `HomeExecutionSession` plus `MonthPlan`:

```swift
struct MonthPlan {
    let needsUpload: Bool
    let needsDownload: Bool
    var phase: Phase = .pending
    var failedItemCount: Int = 0
    var failureMessage: String?
}
```

This preserves the original goal from the old refactor: sync months do not become “done” after upload alone. They pass through `uploadDone` and become `completed` only after their download work finishes.

### 3. Sync months are finalized inline

The biggest behavioral change since the earlier document:

- when a sync month flushes successfully in the upload worker
- `onMonthUploaded` can immediately sync remote data, refresh local data, and download that month’s remote-only items

This means sync work is no longer modeled as a simple “upload phase, then later download phase for all sync months”.

### 4. Connection concerns moved into a dedicated controller

`HomeConnectionController` now handles:

- saved profile loading
- auto-connect
- password prompt flow
- switching/disconnecting profiles
- remote index reload
- recovery of the previous snapshot after a failed connect attempt

That separation substantially reduced connection-specific branching inside the view layer.

### 5. Remote overlay and selection guard are part of the steady-state design

The right-side overlay is now a first-class part of Home:

- `connecting` -> spinner + text
- `disconnected` -> message + “选择存储”
- `connected` -> hidden

Selection is disabled whenever the app is disconnected or already executing.

### 6. Update coalescing moved into the store

Instead of the older “multiple callbacks decide whether to rebuild or invalidate” model, the store now coalesces refresh work:

- reload local data
- sync remote data
- notify connection change
- notify structural change

This avoids cancelling in-flight refresh passes when connection changes and execution changes overlap.

### 7. File-size scan safety fix is now part of the actual implementation

The earlier review called out `Task.detached` usage as a race. The current implementation runs the file-size scan on the main actor and yields between months:

```swift
let task = Task { [weak self] in
    for month in months {
        guard let self, !Task.isCancelled else { return }
        await self.processingWorker.updateFileSize(for: month, cachedSizes: cachedSizes)
        guard !Task.isCancelled else { return }
        self.onFileSizesUpdated?([month])
        await Task.yield()
    }
}
```

That matches the intended refactor outcome.

## Current Invariants

1. `HomeViewController` renders; `HomeScreenStore` owns the stable Home state.
2. `MonthPlan.phase == .completed` means the month is fully done; sync months may sit in `.uploadDone` first.
3. Disconnecting clears selection and can fail an active execution.
4. Download progress is persisted per restored item via hash-index writes.
5. Store refresh work is coalesced instead of repeatedly cancelled/restarted.

## Remaining Risks

1. There is still no automated coverage for the Home execution state machine.
2. Execution, connection changes, and photo-library mutations still interact in subtle ways.
3. Large-library file-size scanning can still take noticeable time, even though the race is fixed.

## Files That Matter Most For The Current Refactor

- `Watermelon/Home/HomeViewController.swift`
- `Watermelon/Home/HomeScreenStore.swift`
- `Watermelon/Home/HomeConnectionController.swift`
- `Watermelon/Home/HomeExecutionCoordinator.swift`
- `Watermelon/Home/HomeExecutionSession.swift`
- `Watermelon/Home/HomeLibraryEngines.swift`
