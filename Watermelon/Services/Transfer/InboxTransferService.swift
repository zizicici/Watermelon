import Foundation
import Photos

struct InboxTransferProgress: Sendable {
    let completedFileCount: Int
    let totalFileCount: Int
}

struct InboxTransferResult: Sendable {
    let fileCount: Int
}

struct InboxTransferPhotoSelectionDetail: Sendable {
    let localIdentifier: String
    let preferredName: String
    let fileSize: Int64?
}

struct InboxTransferFailure: LocalizedError {
    let completedFileCount: Int
    let underlying: Error

    var errorDescription: String? { underlying.localizedDescription }
}

enum InboxTransferServiceError: LocalizedError {
    case noTransferableResources

    var errorDescription: String? {
        switch self {
        case .noTransferableResources:
            return String(localized: "transfer.error.noResources")
        }
    }
}

enum InboxTransferNaming {
    static let directoryName = "Inbox"

    static func directoryPath(basePath: String) -> String {
        RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: directoryName
        )
    }

    static func availableName(
        preferredName: String,
        occupiedCollisionKeys: Set<String>,
        policy: RemoteFileNamePolicy
    ) -> String {
        let sanitized = policy.sanitize(RemotePathBuilder.sanitizeFilename(preferredName))
        return RemoteFileNaming.resolveNextAvailableName(
            baseName: sanitized.isEmpty ? "resource" : sanitized,
            collisionKeys: occupiedCollisionKeys,
            maximumLength: policy.maximumComponentLength
        )
    }
}

final class InboxTransferService: @unchecked Sendable {
    private enum PlannedSource {
        case photoResource(PHAssetResource)
        case file(URL)
    }

    private struct PlannedResource {
        let source: PlannedSource
        let preferredName: String
        let modificationDate: Date?
    }

    private struct PlannedPhotoResource {
        let resource: PHAssetResource
        let preferredName: String
        let modificationDate: Date?
    }

    private let photoLibraryService: PhotoLibraryService
    private let storageClientFactory: StorageClientFactory

    init(
        photoLibraryService: PhotoLibraryService,
        storageClientFactory: StorageClientFactory
    ) {
        self.photoLibraryService = photoLibraryService
        self.storageClientFactory = storageClientFactory
    }

    func transfer(
        items: [InboxTransferItem],
        profile: ServerProfileRecord,
        password: String,
        options: InboxTransferOptions,
        pauseGate: InboxTransferPauseGate,
        onProgress: (@MainActor @Sendable (InboxTransferProgress) -> Void)? = nil,
        onPauseStateChanged: (@MainActor @Sendable (Bool) -> Void)? = nil
    ) async throws -> InboxTransferResult {
        let localIdentifiers = Set(items.compactMap { item -> String? in
            guard case .photoAsset(let localIdentifier) = item else { return nil }
            return localIdentifier
        })
        let assets = photoLibraryService.fetchAssets(localIdentifiers: localIdentifiers)
        var plans = assets.flatMap { asset in
            Self.planPhotoResources(for: asset, options: options).map { plan in
                PlannedResource(
                    source: .photoResource(plan.resource),
                    preferredName: plan.preferredName,
                    modificationDate: plan.modificationDate
                )
            }
        }
        plans.append(contentsOf: items.compactMap { item in
            guard case .file(let file) = item else { return nil }
            return PlannedResource(
                source: .file(file.localURL),
                preferredName: file.preferredName,
                modificationDate: file.modificationDate
            )
        })
        guard !plans.isEmpty else {
            throw InboxTransferServiceError.noTransferableResources
        }

        let client = try storageClientFactory.makeClient(
            profile: profile,
            credentialPayload: password
        )
        do {
            try await client.connect()
            let result = try await transfer(
                plans,
                profile: profile,
                client: client,
                options: options,
                pauseGate: pauseGate,
                onProgress: onProgress,
                onPauseStateChanged: onPauseStateChanged
            )
            await client.disconnectSafely()
            return result
        } catch {
            await client.disconnectSafely()
            throw error
        }
    }

    private func transfer(
        _ plans: [PlannedResource],
        profile: ServerProfileRecord,
        client: any RemoteStorageClientProtocol,
        options: InboxTransferOptions,
        pauseGate: InboxTransferPauseGate,
        onProgress: (@MainActor @Sendable (InboxTransferProgress) -> Void)?,
        onPauseStateChanged: (@MainActor @Sendable (Bool) -> Void)?
    ) async throws -> InboxTransferResult {
        let inboxPath = InboxTransferNaming.directoryPath(basePath: profile.basePath)
        try await client.createDirectory(path: inboxPath)
        let entries = try await client.list(path: inboxPath)
        var collisionKeys = RemoteFileNaming.collisionKeySet(
            from: Set(entries.map(\.name))
        )
        let policy = profile.storageProfile.remoteFileNamePolicy
        var completed = 0

        for plan in plans {
            do {
                try Task.checkCancellation()
                let reachedPause = try await pauseGate.waitUntilResumed {
                    if let onPauseStateChanged {
                        await onPauseStateChanged(true)
                    }
                }
                if reachedPause, let onPauseStateChanged {
                    await onPauseStateChanged(false)
                }
                let fileName = try await transferResource(
                    plan,
                    inboxPath: inboxPath,
                    occupiedCollisionKeys: collisionKeys,
                    policy: policy,
                    client: client,
                    removesLocationMetadata: options.removesLocationMetadata
                )
                collisionKeys.insert(RemoteFileNaming.collisionKey(for: fileName))
                completed += 1
                if let onProgress {
                    await onProgress(InboxTransferProgress(
                        completedFileCount: completed,
                        totalFileCount: plans.count
                    ))
                }
                try Task.checkCancellation()
            } catch {
                throw InboxTransferFailure(
                    completedFileCount: completed,
                    underlying: error
                )
            }
        }

        return InboxTransferResult(fileCount: completed)
    }

    private func transferResource(
        _ plan: PlannedResource,
        inboxPath: String,
        occupiedCollisionKeys: Set<String>,
        policy: RemoteFileNamePolicy,
        client: any RemoteStorageClientProtocol,
        removesLocationMetadata: Bool
    ) async throws -> String {
        try Task.checkCancellation()
        switch plan.source {
        case .file(let localURL):
            return try await uploadPreparedFile(
                localURL,
                preferredName: plan.preferredName,
                modificationDate: plan.modificationDate,
                inboxPath: inboxPath,
                occupiedCollisionKeys: occupiedCollisionKeys,
                policy: policy,
                client: client
            )
        case .photoResource(let resource):
            // Drop Mode exports are independent of the backup iCloud preference.
            let exported = try await photoLibraryService.exportResourceToTempFile(
                resource,
                allowNetworkAccess: true
            )
            defer { try? FileManager.default.removeItem(at: exported) }

            let prepared: URL
            let preferredName: String
            if removesLocationMetadata {
                let sanitized = try await InboxTransferMetadataSanitizer.removingLocationMetadata(
                    from: exported,
                    resourceType: resource.type
                )
                prepared = sanitized.url
                preferredName = Self.replacingFilenameExtension(
                    plan.preferredName,
                    with: sanitized.filenameExtension
                )
            } else {
                prepared = exported
                preferredName = plan.preferredName
            }
            defer {
                if prepared != exported {
                    try? FileManager.default.removeItem(at: prepared)
                }
            }

            if let modificationDate = plan.modificationDate {
                try? FileManager.default.setAttributes(
                    [.modificationDate: modificationDate],
                    ofItemAtPath: prepared.path
                )
            }

            return try await uploadPreparedFile(
                prepared,
                preferredName: preferredName,
                modificationDate: plan.modificationDate,
                inboxPath: inboxPath,
                occupiedCollisionKeys: occupiedCollisionKeys,
                policy: policy,
                client: client
            )
        }
    }

    private func uploadPreparedFile(
        _ localURL: URL,
        preferredName: String,
        modificationDate: Date?,
        inboxPath: String,
        occupiedCollisionKeys: Set<String>,
        policy: RemoteFileNamePolicy,
        client: any RemoteStorageClientProtocol
    ) async throws -> String {
        var collisionKeys = occupiedCollisionKeys
        var collisionAttempt = 0
        while true {
            try Task.checkCancellation()
            let fileName = InboxTransferNaming.availableName(
                preferredName: preferredName,
                occupiedCollisionKeys: collisionKeys,
                policy: policy
            )
            let remotePath = RemotePathBuilder.absolutePath(
                basePath: inboxPath,
                remoteRelativePath: fileName
            )
            do {
                try await client.upload(
                    localURL: localURL,
                    remotePath: remotePath,
                    mode: .createIfAbsent,
                    respectTaskCancellation: true,
                    onProgress: nil
                )
                if let modificationDate,
                   client.shouldSetModificationDate() {
                    try? await client.setModificationDate(modificationDate, forPath: remotePath)
                }
                return fileName
            } catch {
                guard remoteStorageIsNameCollision(error), collisionAttempt < 100 else {
                    throw error
                }
                collisionKeys.insert(RemoteFileNaming.collisionKey(for: fileName))
                collisionAttempt += 1
            }
        }
    }

    static func replacingFilenameExtension(_ filename: String, with filenameExtension: String?) -> String {
        let path = filename as NSString
        let stem = path.deletingPathExtension
        guard let filenameExtension, !filenameExtension.isEmpty else { return stem }
        return (stem as NSString).appendingPathExtension(filenameExtension) ?? stem
    }

    static func photoSelectionDetails(
        localIdentifiers: Set<String>,
        options: InboxTransferOptions
    ) -> [InboxTransferPhotoSelectionDetail] {
        guard !localIdentifiers.isEmpty else { return [] }
        let result = PHAsset.fetchAssets(
            withLocalIdentifiers: Array(localIdentifiers),
            options: nil
        )
        var details: [InboxTransferPhotoSelectionDetail] = []
        details.reserveCapacity(result.count)
        for index in 0 ..< result.count {
            guard !Task.isCancelled else { return [] }
            let asset = result.object(at: index)
            let plans = planPhotoResources(for: asset, options: options)
            guard let preferredName = plans.first?.preferredName else { continue }
            var totalSize: Int64 = 0
            var hasKnownSize = true
            for plan in plans {
                let resourceSize = PhotoLibraryService.resourceFileSize(plan.resource)
                guard resourceSize > 0 else {
                    hasKnownSize = false
                    continue
                }
                let (sum, overflowed) = totalSize.addingReportingOverflow(resourceSize)
                if overflowed {
                    hasKnownSize = false
                } else {
                    totalSize = sum
                }
            }
            details.append(InboxTransferPhotoSelectionDetail(
                localIdentifier: asset.localIdentifier,
                preferredName: preferredName,
                fileSize: hasKnownSize ? totalSize : nil
            ))
        }
        return details
    }

    private static func planPhotoResources(
        for asset: PHAsset,
        options: InboxTransferOptions
    ) -> [PlannedPhotoResource] {
        let available = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
            from: PHAssetResource.assetResources(for: asset)
        )
        let kind: InboxTransferAssetKind = switch asset.mediaType {
        case .image:
            .image(isLivePhoto: PhotoLibraryService.isLivePhoto(asset))
        case .video:
            .video
        default:
            .other
        }
        let selectedResourceIndices = Set(InboxTransferResourcePolicy.select(
            available.map {
                InboxTransferResourceCandidate(identifier: $0.resourceIndex, role: $0.role)
            },
            kind: kind,
            options: options
        ))
        let selected = available.filter { selectedResourceIndices.contains($0.resourceIndex) }
        guard !selected.isEmpty else { return [] }
        let identities = selected.map {
            RemoteFileNaming.ResourceIdentity(
                role: $0.role,
                slot: $0.slot,
                originalFilename: PhotoLibraryService.safeOriginalFilename(for: $0.resource)
            )
        }
        let preferredStem = RemoteFileNaming.preferredAssetNameStem(
            orderedResources: identities,
            fallbackTimestampMs: LibraryCreationDate.normalized(asset.creationDate).milliseconds
        )
        let modificationDate = asset.creationDate ?? asset.modificationDate
        return zip(selected, identities).map { selected, identity in
            PlannedPhotoResource(
                resource: selected.resource,
                preferredName: RemoteFileNaming.preferredRemoteFileName(
                    preferredAssetNameStem: preferredStem,
                    resource: identity
                ),
                modificationDate: modificationDate
            )
        }
    }
}

enum InboxTransferAssetKind: Equatable, Sendable {
    case image(isLivePhoto: Bool)
    case video
    case other
}

struct InboxTransferResourceCandidate: Equatable, Sendable {
    let identifier: Int
    let role: Int
}

enum InboxTransferResourcePolicy {
    static func select(
        _ candidates: [InboxTransferResourceCandidate],
        kind: InboxTransferAssetKind,
        options: InboxTransferOptions
    ) -> [Int] {
        var selected = Set<Int>()
        switch kind {
        case .image(let isLivePhoto):
            selectPhotoSide(candidates, options: options, into: &selected)
            if isLivePhoto && options.includesLivePhotoVideo {
                let roles = options.usesOriginalEditedVideo
                    ? [ResourceTypeCode.pairedVideo, ResourceTypeCode.fullSizePairedVideo,
                       ResourceTypeCode.adjustmentBasePairedVideo]
                    : [ResourceTypeCode.fullSizePairedVideo, ResourceTypeCode.pairedVideo,
                       ResourceTypeCode.adjustmentBasePairedVideo]
                selectFirstAvailableRole(roles, from: candidates, into: &selected)
            }
        case .video:
            let roles = options.usesOriginalEditedVideo
                ? [ResourceTypeCode.video, ResourceTypeCode.fullSizeVideo,
                   ResourceTypeCode.adjustmentBaseVideo]
                : [ResourceTypeCode.fullSizeVideo, ResourceTypeCode.video,
                   ResourceTypeCode.adjustmentBaseVideo]
            selectFirstAvailableRole(roles, from: candidates, into: &selected)
        case .other:
            selected.formUnion(candidates.lazy.filter {
                ResourceRole.isDisplayableMedia($0.role)
            }.map(\.identifier))
        }
        return candidates.compactMap { selected.contains($0.identifier) ? $0.identifier : nil }
    }

    private static func selectPhotoSide(
        _ candidates: [InboxTransferResourceCandidate],
        options: InboxTransferOptions,
        into selected: inout Set<Int>
    ) {
        if options.usesOriginalEditedPhoto {
            let originals = candidates.filter {
                $0.role == ResourceTypeCode.photo || $0.role == ResourceTypeCode.alternatePhoto
            }
            if !originals.isEmpty {
                selected.formUnion(originals.map(\.identifier))
                return
            }
            selectFirstAvailableRole(
                [ResourceTypeCode.fullSizePhoto, ResourceTypeCode.adjustmentBasePhoto],
                from: candidates,
                into: &selected
            )
            return
        }

        if candidates.contains(where: { $0.role == ResourceTypeCode.fullSizePhoto }) {
            selectFirstAvailableRole(
                [ResourceTypeCode.fullSizePhoto],
                from: candidates,
                into: &selected
            )
            return
        }
        let originals = candidates.filter {
            $0.role == ResourceTypeCode.photo || $0.role == ResourceTypeCode.alternatePhoto
        }
        if !originals.isEmpty {
            selected.formUnion(originals.map(\.identifier))
            return
        }
        selectFirstAvailableRole(
            [ResourceTypeCode.adjustmentBasePhoto],
            from: candidates,
            into: &selected
        )
    }

    private static func selectFirstAvailableRole(
        _ roles: [Int],
        from candidates: [InboxTransferResourceCandidate],
        into selected: inout Set<Int>
    ) {
        guard let role = roles.first(where: { preferredRole in
            candidates.contains(where: { $0.role == preferredRole })
        }) else { return }
        selected.formUnion(candidates.lazy.filter { $0.role == role }.map(\.identifier))
    }
}
