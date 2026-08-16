import UIKit

nonisolated struct GoogleDriveProfileSetupDraft: Sendable {
    let connectionParams: GoogleDriveConnectionParams
    let credentialJSONString: String
    let username: String?
    let refreshTokenExpiresAt: Date?
}

@MainActor
final class GoogleDriveProfileSetupCoordinator {
    private let authenticationService: GoogleDriveOAuthService
    private let tokenService: GoogleDriveTokenService
    private let sharedState: GoogleDriveSharedState

    nonisolated init(
        authenticationService: GoogleDriveOAuthService,
        tokenService: GoogleDriveTokenService,
        sharedState: GoogleDriveSharedState = GoogleDriveSharedState()
    ) {
        self.authenticationService = authenticationService
        self.tokenService = tokenService
        self.sharedState = sharedState
    }

    func prepare(
        clientID: String,
        from parent: UIViewController,
        forceReauthentication: Bool
    ) async throws -> GoogleDriveProfileSetupDraft {
        let signIn = try await authenticationService.signIn(
            clientID: clientID,
            from: parent,
            forceReauthentication: forceReauthentication
        )
        try Task.checkCancellation()
        let bootstrap = GoogleDriveAppFolderBootstrapService(
            clientID: signIn.clientID,
            credential: signIn.credential,
            tokenProvider: tokenService,
            sharedState: sharedState
        )
        let root = try await bootstrap.resolveOrCreateRoot()
        let params = GoogleDriveConnectionParams(
            clientID: signIn.clientID,
            accountSubject: signIn.credential.accountSubject,
            rootFolderID: root.rootFolderID,
            lockRootSlotID: root.lockRootSlotID,
            displayRootPath: root.displayRootPath
        )
        let connection = try CanonicalGoogleDriveConnection(params: params)
        let client = GoogleDriveClient(
            config: GoogleDriveClient.Config(connection: connection),
            credential: signIn.credential,
            tokenProvider: tokenService,
            sharedState: sharedState
        )
        try await client.connect()
        try await client.verifyWriteAccess()
        try Task.checkCancellation()
        return GoogleDriveProfileSetupDraft(
            connectionParams: params,
            credentialJSONString: try signIn.credential.encodedJSONString(),
            username: signIn.accountDisplayName,
            refreshTokenExpiresAt: signIn.refreshTokenExpiresAt
        )
    }

    func cancelInteractiveSignIn() {
        authenticationService.cancelInteractiveSignIn()
    }
}

actor GoogleDriveAppFolderBootstrapService {
    struct Result: Sendable {
        let rootFolderID: String
        let lockRootSlotID: String
        let displayRootPath: String
    }

    private static let rootSettleAttempts = 6
    private static let rootSettleDelay = Duration.milliseconds(500)
    private static let rootFolderName = "Watermelon Backup"
    private let transport: GoogleDriveTransport
    private let sharedState: GoogleDriveSharedState
    private let clientID: String
    private let accountSubject: String

    init(
        clientID: String,
        credential: GoogleDriveCredentialBlob,
        tokenProvider: any GoogleDriveAccessTokenProviding,
        sharedState: GoogleDriveSharedState,
        sessionConfiguration: URLSessionConfiguration? = nil
    ) {
        self.clientID = clientID
        accountSubject = credential.accountSubject
        self.sharedState = sharedState
        transport = GoogleDriveTransport(
            clientID: clientID,
            credential: credential,
            tokenProvider: tokenProvider,
            sharedState: sharedState,
            sessionConfiguration: sessionConfiguration
        )
    }

    func resolveOrCreateRoot() async throws -> Result {
        let key = GoogleDriveDirectoryMutationGate.Key(
            accountSubject: accountSubject,
            rootFolderID: "bootstrap:\(clientID.lowercased())"
        )
        try await sharedState.directoryMutationGate.acquire(key)
        do {
            try Task.checkCancellation()
            let result = try await resolveOrCreateRootUncoordinated()
            await sharedState.directoryMutationGate.release(key)
            return result
        } catch {
            await sharedState.directoryMutationGate.release(key)
            throw error
        }
    }

    private func resolveOrCreateRootUncoordinated() async throws -> Result {
        let discoveredRoots = try await findRoots()
        var roots = try await reconcileRoots(discoveredRoots)
        let requiresSettle = roots.isEmpty || discoveredRoots.count > 1
        var createdRootID: String?
        if roots.isEmpty {
            let rootID = try await transport.generateFileIDs(count: 1)[0]
            let lockRootSlotID = try await transport.generateFileIDs(
                count: 1,
                space: .appDataFolder
            )[0]
            try await createRoot(rootID: rootID, lockRootSlotID: lockRootSlotID)
            createdRootID = rootID
        }
        if requiresSettle {
            roots = try await settleRoots()
        }
        guard var root = roots.first else { throw RemoteStorageClientError.unavailable }
        if root.id != createdRootID, try await lockControlFiles(rootID: root.id).isEmpty {
            root = try await rotateLockRootSlot(in: root)
        }
        guard let lockRootSlotID = root.watermelonLockRootSlotID else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return Result(
            rootFolderID: root.id,
            lockRootSlotID: lockRootSlotID,
            displayRootPath: "/\(root.name ?? Self.rootFolderName)"
        )
    }

    private func reconcileRoots(_ roots: [GoogleDriveFile]) async throws -> [GoogleDriveFile] {
        guard roots.allSatisfy({ $0.watermelonLockRootSlotID != nil }) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        guard roots.count > 1 else { return roots }
        let ordered = roots.sorted(by: googleDriveFolderPrecedes)
        for loser in ordered.dropFirst() {
            guard try await transport.hasChildren(parentID: loser.id) == false else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            do {
                try await transport.deleteFile(id: loser.id)
            } catch {
                if !GoogleDriveErrorClassifier.isNotFound(error) { throw error }
            }
        }
        return [ordered[0]]
    }

    private func settleRoots() async throws -> [GoogleDriveFile] {
        var roots: [GoogleDriveFile] = []
        var previousRootID: String?
        for attempt in 0 ..< Self.rootSettleAttempts {
            if attempt > 0 { try await Task.sleep(for: Self.rootSettleDelay) }
            roots = try await reconcileRoots(try await findRoots())
            guard let root = roots.first else { continue }
            if previousRootID == root.id { return roots }
            previousRootID = root.id
        }
        guard roots.count == 1 else { throw RemoteStorageClientError.unavailable }
        return roots
    }

    private func lockControlFiles(rootID: String) async throws -> [GoogleDriveFile] {
        try await transport.listFiles(
            query: "appProperties has { key='\(GoogleDriveConstants.lockRepoRootIDKey)' and value='\(Self.escapeQuery(rootID))' } and trashed = false",
            space: .appDataFolder
        )
    }

    private func rotateLockRootSlot(in root: GoogleDriveFile) async throws -> GoogleDriveFile {
        let lockRootSlotID = try await transport.generateFileIDs(count: 1, space: .appDataFolder)[0]
        var appProperties = root.appProperties ?? [:]
        appProperties[GoogleDriveConstants.rootRoleKey] = GoogleDriveConstants.rootRole
        appProperties[GoogleDriveConstants.rootSchemaKey] = GoogleDriveConstants.rootSchemaVersion
        appProperties[GoogleDriveConstants.lockRootSlotKey] = lockRootSlotID
        let updated = try await transport.updateAppProperties(id: root.id, appProperties: appProperties)
        guard updated.id == root.id,
              updated.watermelonLockRootSlotID == lockRootSlotID else {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
        return updated
    }

    private func findRoots() async throws -> [GoogleDriveFile] {
        try await transport.listFiles(
            query: "appProperties has { key='\(GoogleDriveConstants.rootRoleKey)' and value='\(GoogleDriveConstants.rootRole)' } and trashed = false"
        )
    }

    private func createRoot(rootID: String, lockRootSlotID: String) async throws {
        let appProperties = [
            GoogleDriveConstants.rootRoleKey: GoogleDriveConstants.rootRole,
            GoogleDriveConstants.rootSchemaKey: GoogleDriveConstants.rootSchemaVersion,
            GoogleDriveConstants.lockRootSlotKey: lockRootSlotID
        ]
        do {
            let created = try await transport.createFolder(
                id: rootID,
                name: Self.rootFolderName,
                parentID: "root",
                appProperties: appProperties
            )
            guard Self.isValidRoot(
                created,
                rootID: rootID,
                lockRootSlotID: lockRootSlotID
            ) else {
                throw GoogleDriveAuthenticationError.invalidResponse
            }
        } catch {
            let createError = error
            guard GoogleDriveErrorClassifier.isMutationOutcomeUnknown(createError)
                || GoogleDriveErrorClassifier.isNameCollision(createError) else {
                throw createError
            }
            guard let recovered = try await transport.file(id: rootID) else { throw createError }
            guard Self.isValidRoot(
                recovered,
                rootID: rootID,
                lockRootSlotID: lockRootSlotID
            ) else {
                throw RemoteStorageClientError.invalidConfiguration
            }
        }
    }

    private nonisolated static func isValidRoot(
        _ root: GoogleDriveFile,
        rootID: String,
        lockRootSlotID: String
    ) -> Bool {
        root.id == rootID
            && root.watermelonLockRootSlotID == lockRootSlotID
    }

    private nonisolated static func escapeQuery(_ value: String) -> String {
        value.replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "'", with: "\\'")
    }

}
