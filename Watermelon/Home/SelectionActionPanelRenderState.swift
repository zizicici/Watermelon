import UIKit

enum SelectionActionPanelPrimaryAction: Equatable {
    case none
    case execute
    case pause
    case resume
    case complete
}

enum SelectionActionPanelSecondaryAction: Equatable {
    case stop
}

enum SelectionActionPanelButtonStyle: Equatable {
    case execute
    case pause
    case resume
    case complete
    case close
    case failed
    case stop
}

struct SelectionActionPanelButtonState: Equatable {
    let style: SelectionActionPanelButtonStyle
    let isEnabled: Bool
    let showsSpinner: Bool
    let isHidden: Bool
}

struct SelectionActionPanelSelectionState: Equatable {
    let backupCount: Int
    let downloadCount: Int
    let syncCount: Int
}

struct SelectionActionPanelExecutionState: Equatable {
    let uploadCount: Int
    let downloadCount: Int
    let syncCount: Int
    let statusText: String
    let hasLogAlert: Bool
    let primaryAction: SelectionActionPanelPrimaryAction
    let primaryButton: SelectionActionPanelButtonState
    let stopAction: SelectionActionPanelSecondaryAction?
    let stopButton: SelectionActionPanelButtonState?
}

enum SelectionActionPanelViewState: Equatable {
    case selection(SelectionActionPanelSelectionState)
    case execution(SelectionActionPanelExecutionState)

    var primaryAction: SelectionActionPanelPrimaryAction {
        switch self {
        case .selection:
            return .execute
        case .execution(let state):
            return state.primaryAction
        }
    }
}

struct SelectionActionPanelMenus {
    // Menus intentionally live outside the Equatable render state.
    // SelectionActionPanel always reapplies them before diffing state because UIMenu is not Equatable.
    let backup: UIMenu?
    let download: UIMenu?
    let sync: UIMenu?

    static let empty = SelectionActionPanelMenus(
        backup: nil,
        download: nil,
        sync: nil
    )
}

enum SelectionActionPanelViewStateBuilder {
    static func selection(backupCount: Int, downloadCount: Int, syncCount: Int) -> SelectionActionPanelViewState {
        .selection(
            SelectionActionPanelSelectionState(
                backupCount: backupCount,
                downloadCount: downloadCount,
                syncCount: syncCount
            )
        )
    }

    static func execution(from executionState: HomeExecutionState) -> SelectionActionPanelViewState {
        let primaryAction: SelectionActionPanelPrimaryAction
        let primaryButton: SelectionActionPanelButtonState
        let stopAction: SelectionActionPanelSecondaryAction?
        let stopButton: SelectionActionPanelButtonState?
        let hasLogAlert = hasLogAlert(for: executionState)

        switch executionState.controlState {
        case .starting, .resuming:
            primaryAction = .none
            primaryButton = SelectionActionPanelButtonState(
                style: .execute,
                isEnabled: false,
                showsSpinner: true,
                isHidden: false
            )
            stopAction = .stop
            stopButton = SelectionActionPanelButtonState(
                style: .stop,
                isEnabled: true,
                showsSpinner: false,
                isHidden: false
            )

        case .pausing:
            primaryAction = .none
            primaryButton = SelectionActionPanelButtonState(
                style: .pause,
                isEnabled: false,
                showsSpinner: true,
                isHidden: false
            )
            stopAction = .stop
            stopButton = SelectionActionPanelButtonState(
                style: .stop,
                isEnabled: true,
                showsSpinner: false,
                isHidden: false
            )

        case .stopping:
            primaryAction = .none
            primaryButton = SelectionActionPanelButtonState(
                style: primaryButtonStyle(for: executionState.phase),
                isEnabled: false,
                showsSpinner: false,
                isHidden: false
            )
            stopAction = .stop
            stopButton = SelectionActionPanelButtonState(
                style: .stop,
                isEnabled: false,
                showsSpinner: true,
                isHidden: false
            )

        case .idle:
            switch executionState.phase {
            case .uploading, .downloading:
                primaryAction = .pause
                primaryButton = SelectionActionPanelButtonState(
                    style: .pause,
                    isEnabled: true,
                    showsSpinner: false,
                    isHidden: false
                )
                stopAction = .stop
                stopButton = SelectionActionPanelButtonState(
                    style: .stop,
                    isEnabled: true,
                    showsSpinner: false,
                    isHidden: false
                )

            case .uploadPaused, .downloadPaused:
                primaryAction = .resume
                primaryButton = SelectionActionPanelButtonState(
                    style: .resume,
                    isEnabled: true,
                    showsSpinner: false,
                    isHidden: false
                )
                stopAction = .stop
                stopButton = SelectionActionPanelButtonState(
                    style: .stop,
                    isEnabled: true,
                    showsSpinner: false,
                    isHidden: false
                )

            case .completed:
                primaryAction = .complete
                primaryButton = SelectionActionPanelButtonState(
                    style: .complete,
                    isEnabled: true,
                    showsSpinner: false,
                    isHidden: false
                )
                stopAction = nil
                stopButton = nil

            case .failed:
                primaryAction = .complete
                primaryButton = SelectionActionPanelButtonState(
                    style: .close,
                    isEnabled: true,
                    showsSpinner: false,
                    isHidden: false
                )
                stopAction = nil
                stopButton = nil
            }
        }

        return .execution(
            SelectionActionPanelExecutionState(
                uploadCount: executionState.uploadMonths.count,
                downloadCount: executionState.downloadMonths.count,
                syncCount: executionState.syncMonths.count,
                statusText: executionState.statusText,
                hasLogAlert: hasLogAlert,
                primaryAction: primaryAction,
                primaryButton: primaryButton,
                stopAction: stopAction,
                stopButton: stopButton
            )
        )
    }

    private static func hasLogAlert(for executionState: HomeExecutionState) -> Bool {
        if !executionState.failedMonthInfos.isEmpty {
            return true
        }
        if case .failed = executionState.phase {
            return true
        }
        return false
    }

    private static func primaryButtonStyle(for phase: ExecutionPhase) -> SelectionActionPanelButtonStyle {
        switch phase {
        case .uploading, .downloading:
            return .pause
        case .uploadPaused, .downloadPaused:
            return .resume
        case .completed:
            return .complete
        case .failed:
            return .failed
        }
    }
}
