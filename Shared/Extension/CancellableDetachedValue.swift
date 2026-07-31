import Foundation

func withCancellableDetachedValue<Value: Sendable>(
    priority: TaskPriority? = nil,
    operation: @escaping @Sendable () -> Value
) async -> Value {
    let task = Task.detached(
        priority: priority,
        operation: operation
    )
    return await withTaskCancellationHandler {
        await task.value
    } onCancel: {
        task.cancel()
    }
}

func withCancellableDetachedAsyncValue<Value: Sendable>(
    priority: TaskPriority? = nil,
    operation: @escaping @Sendable () async -> Value
) async -> Value {
    let task = Task.detached(priority: priority) {
        await operation()
    }
    return await withTaskCancellationHandler {
        await task.value
    } onCancel: {
        task.cancel()
    }
}
