import Foundation

final class HomeExecutionTerminationBridge {
    let control: ExecutionTerminationControl

    init(control: ExecutionTerminationControl = ExecutionTerminationControl()) {
        self.control = control
    }

    var shouldDrain: Bool {
        control.shouldDrain
    }

    func request(_ intent: ExecutionTerminationIntent) {
        control.request(intent)
    }

    func resolveUploadResult(
        _ result: BackupSessionAsyncBridge.UploadResult,
        session: inout HomeExecutionSession
    ) -> HomeExecutionSession.UploadResultOutcome {
        session.handleUploadResult(
            result,
            terminationIntent: control.terminationIntent
        )
    }
}
