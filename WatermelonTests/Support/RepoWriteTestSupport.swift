import Foundation
@testable import Watermelon

extension RemoteLiteRepoGateway {
    static func prepareForegroundWrite(
        client: any RemoteStorageClientProtocol,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool = false,
        basePath: String,
        writerID: String?,
        allowsFreshOwnLockTakeover: Bool = false,
        freshOwnLockTakeoverScopes: Set<String> = [],
        ownLockTakeoverScope: String? = nil,
        now: Date = Date(),
        initialDecision: RepoFormatDecision? = nil,
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> WritePlan {
        try await prepareForegroundWrite(
            client: client,
            lockClientHandle: LiteLockClientHandle(client: lockClient, ownsClient: ownsLockClient),
            basePath: basePath,
            writerID: writerID,
            allowsFreshOwnLockTakeover: allowsFreshOwnLockTakeover,
            freshOwnLockTakeoverScopes: freshOwnLockTakeoverScopes,
            ownLockTakeoverScope: ownLockTakeoverScope,
            now: now,
            initialDecision: initialDecision,
            reconnectLockClient: reconnectLockClient,
            onForeignWriterObserved: onForeignWriterObserved,
            leaseDiagnosticLogger: leaseDiagnosticLogger,
            onMigrationProgress: onMigrationProgress
        )
    }

    static func prepareBackgroundWrite(
        client: any RemoteStorageClientProtocol,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool = false,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> BackgroundOutcome {
        try await prepareBackgroundWrite(
            client: client,
            lockClientHandle: LiteLockClientHandle(client: lockClient, ownsClient: ownsLockClient),
            basePath: basePath,
            writerID: writerID,
            now: now,
            reconnectLockClient: reconnectLockClient,
            onForeignWriterObserved: onForeignWriterObserved,
            leaseDiagnosticLogger: leaseDiagnosticLogger,
            onMigrationProgress: onMigrationProgress
        )
    }

    static func prepareMaintenance(
        client: any RemoteStorageClientProtocol,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool = false,
        basePath: String,
        writerID: String?,
        allowsFreshOwnLockTakeover: Bool = false,
        freshOwnLockTakeoverScopes: Set<String> = [],
        ownLockTakeoverScope: String? = nil,
        now: Date = Date(),
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        try await prepareMaintenance(
            client: client,
            lockClientHandle: LiteLockClientHandle(client: lockClient, ownsClient: ownsLockClient),
            basePath: basePath,
            writerID: writerID,
            allowsFreshOwnLockTakeover: allowsFreshOwnLockTakeover,
            freshOwnLockTakeoverScopes: freshOwnLockTakeoverScopes,
            ownLockTakeoverScope: ownLockTakeoverScope,
            now: now,
            reconnectLockClient: reconnectLockClient,
            onForeignWriterObserved: onForeignWriterObserved,
            leaseDiagnosticLogger: leaseDiagnosticLogger,
            onMigrationProgress: onMigrationProgress
        )
    }
}

enum RepoLeaseGuard {
    static func assertLeaseConfidence(_ session: RepoLeaseSession?, now: Date = Date()) async throws {
        try await session?.assertLeaseConfidence(now: now)
    }

    static func assertOwnedBeforeFlush(_ session: RepoLeaseSession?, now: Date = Date()) async throws {
        try await session?.assertLeaseProvenForWrite(now: now)
    }
}
