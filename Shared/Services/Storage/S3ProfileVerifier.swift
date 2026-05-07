import Foundation

enum S3ProfileVerifier {
    static let errorDomain = "S3ProfileVerifier"

    static func run(
        client: any RemoteStorageClientProtocol,
        writeAccessMessageTemplate: String
    ) async throws {
        do {
            try await client.connect()
        } catch {
            await client.disconnect()
            try rethrowIfCancelled(error)
            throw failure(message: S3ErrorClassifier.describe(error))
        }
        do {
            try await client.verifyWriteAccess()
        } catch {
            await client.disconnect()
            try rethrowIfCancelled(error)
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
