import Foundation

enum S3ProfileVerifier {
    static let errorDomain = "S3ProfileVerifier"
    static let verificationTimeout: TimeInterval = 90

    static func run(
        client: any RemoteStorageClientProtocol,
        writeAccessMessageTemplate: String,
        timeout: TimeInterval = verificationTimeout
    ) async throws {
        let outcome = await NetworkRecovery.boundedAttempt(
            deadline: Date().addingTimeInterval(max(0, timeout)),
            onAbandon: { client.cancelActiveOperationsForAbandonment() },
            reap: { (_: Result<Void, Error>) in await client.reapAbandonedOperations() },
            op: { () async -> Result<Void, Error> in
                do {
                    try await perform(
                        client: client,
                        writeAccessMessageTemplate: writeAccessMessageTemplate
                    )
                    return .success(())
                } catch {
                    return .failure(error)
                }
            }
        )
        switch outcome {
        case .completed(.success):
            return
        case .completed(.failure(let error)):
            throw error
        case .timedOut:
            if Task.isCancelled { throw CancellationError() }
            let formatted = String.localizedStringWithFormat(
                writeAccessMessageTemplate,
                S3ErrorClassifier.describe(RemoteStorageClientError.unavailable)
            )
            throw failure(message: formatted)
        }
    }

    private static func perform(
        client: any RemoteStorageClientProtocol,
        writeAccessMessageTemplate: String
    ) async throws {
        do {
            try await client.connect()
            try Task.checkCancellation()
        } catch {
            try rethrowIfCancelled(error)
            await client.disconnect()
            throw failure(message: S3ErrorClassifier.describe(error))
        }
        do {
            try await client.verifyWriteAccess()
            try Task.checkCancellation()
        } catch {
            try rethrowIfCancelled(error)
            await client.disconnect()
            let formatted = String.localizedStringWithFormat(
                writeAccessMessageTemplate,
                S3ErrorClassifier.describe(error)
            )
            throw failure(message: formatted)
        }
        await client.disconnect()
    }

    private static func rethrowIfCancelled(_ error: Error) throws {
        if Task.isCancelled || error is CancellationError { throw error }
    }

    private static func failure(message: String) -> NSError {
        NSError(
            domain: errorDomain,
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: message]
        )
    }
}
