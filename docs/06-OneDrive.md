# OneDrive Personal

## Scope

The first release supports OneDrive Personal through Microsoft Graph. OneDrive for Business and SharePoint-backed drives remain unavailable until they have a separate live validation matrix.

The app requests only the delegated `Files.ReadWrite.AppFolder` preview scope. Graph creates the app folder on first access, normally under `OneDrive/Apps/<application name>`. Watermelon stores `driveID` and the app-folder `rootItemID`; all backup paths are relative to that root.

References:

- [Microsoft Graph app folder](https://learn.microsoft.com/en-us/graph/onedrive-sharepoint-appfolder)
- [Microsoft Graph permissions](https://learn.microsoft.com/en-us/graph/permissions-reference#filesreadwriteappfolder)

## Architecture

- `OneDriveMSALService` is iOS-only and owns interactive and silent MSAL token acquisition.
- Shared code depends on `OneDriveAccessTokenProviding`; it never imports MSAL.
- `OneDriveAppFolderBootstrapService` is iOS-only setup code and resolves `approot`, `driveID`, and `rootItemID` after interactive sign-in.
- `OneDriveGraphTransport` is the single Graph HTTP path for bootstrap and data-plane requests; it owns authentication retry, redirect stripping, throttling, and upload/download stall watchdog integration.
- `OneDriveSharedState` carries profile-namespaced transient DriveItem metadata so clients created for the same OneDrive profile can reuse item IDs returned by list/upload/move without crossing accounts or app-folder roots.
- `OneDriveClient` implements `RemoteStorageClientProtocol` with Graph REST requests, but hot repository writes use OneDrive-only capabilities where Graph item IDs are safer and cheaper than repeated path probes.
- `RemoteIndexSyncService` keeps a OneDrive-only persisted Lite manifest snapshot cache. Connect still scans remote manifest metadata before declaring the repository ready, but unchanged months are restored from the local snapshot instead of downloading every month sqlite again.
- `StorageClientFactory` constructs OneDrive only when a token provider is injected. The macOS migration target uses no provider and cannot execute this backend.
- The existing Keychain service stores only account/tenant/environment identity metadata. MSAL stores refresh and access tokens in its own cache. No Microsoft password or client secret is stored.
- `OneDriveProfileSetupCoordinator` owns sign-in, app-folder bootstrap, and the write probe. It returns a pending account lease to the screen; save commits the lease, while failure, cancellation, replacement, or departure discards it after probe cleanup. If a timed-out network operation does not cooperate with cancellation, the UI returns while the lease is transferred to its late reaper and final cleanup. Pending leases are process-wide and reference-counted by MSAL home-account identifier, so overlapping setup attempts and background containers cannot remove each other's cache entry. Startup and failed-sign-in reconciliation remove cache-only accounts left by interrupted setup, while saved accounts remain cached until the last profile that references the account is deleted or changed.

The credential pins `homeAccountIdentifier`, tenant ID, and authority environment. The connection pins cloud environment, account type, drive ID, and app-folder root item ID. Ordinary item IDs are cached only as transient profile-scoped observations and are invalidated after local move/delete operations; `metadata` and `exists` still use live path lookups.

## Graph behavior

- Listing follows complete opaque `@odata.nextLink` values only after validating that they remain HTTPS, same-host, same-default-port, and inside the configured Graph version path.
- Directory listing uses the Graph path form relative to the pinned app-folder root item (`items/{rootItemID}:/path:/children`) instead of first resolving the folder to an item ID. This keeps Lite manifest scanning to the actual `children` call plus pagination.
- Replace and conditional-create uploads below 10 MiB use direct content upload. Conditional create sends `@microsoft.graph.conflictBehavior = fail`.
- Larger uploads use an upload session with `@microsoft.graph.conflictBehavior = fail` for conditional create and `replace` for overwrite. Because the current integration is Personal-only, the session request also sends `fileSize` so quota failures can return before bytes are uploaded. The request omits `deferCommit`, so the final fragment commits automatically.
- Resource uploads in the backup hot path use conditional create. If an unmanifested same-name file already exists, Graph collision handling feeds the normal rename retry instead of overwriting that remote file.
- Uploads use the same app-folder-root-relative item path (`items/{rootItemID}:/YYYY/MM/file:/content` or `:/createUploadSession`) and do not pre-resolve the parent item ID. Destination folders are created lazily only after Graph returns not-found for the upload path, so a seeded upload month does not need a pre-upload `LIST YYYY/MM` or metadata lookup just to discover whether the data directory exists.
- Upload fragments are at most 10 MiB. `nextExpectedRanges` supplies the next missing start offset; its finite end never defines fragment shape. Upload-session URLs are preauthenticated and never receive an `Authorization` header. Transient fragment failures query the same session and resume from its reported offset with bounded retries. Upload PUT and status requests also have body/response stall watchdogs so a connected but frozen transfer cannot occupy a worker indefinitely.
- Graph item IDs are cached only as an acceleration for operations that require IDs, such as move, delete, read-back, and `setModificationDate`. Hot resource upload is path-addressed from the pinned app-folder root to avoid stale-parent-ID retries.
- The app disables CFNetwork subsystem logging for its process because Foundation can otherwise record a failed preauthenticated URL before application-level error sanitization runs. Watermelon's own OneDrive HTTP traces also strip query strings and fragments from every logged URL.
- Download uses Graph `/content` through the common URLSession stall watchdog. Cross-origin redirects strip `Authorization`, and HTTPS downgrade redirects are rejected.
- The generic backup hot path does not issue a post-upload modification-time PATCH for OneDrive. Restore semantics come from the manifest; `setModificationDate` remains available as a best-effort API for explicit callers.
- Delete uses the normal Graph delete operation and therefore the OneDrive recycle bin.
- Copy remains implemented for compatibility and repair/migration-style callers, but it is not used by the backup hot path. OneDrive Consumer does not support COPY conflict behavior, so replace semantics first remove an existing destination, submit COPY, accept both `200` and `202` monitor reports, and verify the completed `resourceId` or destination. Monitoring and final verification continue after caller cancellation once Graph has accepted the operation. A missing, timed-out, or unknown result is never reported as success.
- OneDrive-only filename restrictions are applied before collision checks and manifest insertion; reserved device names, `_vti_`, invalid edge characters, and the 255-character component limit share one policy with client-side path validation. Generic backup and restore naming remains unchanged for other backends and existing repositories.
- For OneDrive, a same-name remote resource that is not represented by the manifest is treated as a collision and the new upload is renamed. The app does not download that remote file just to compute a secondary hash; the manifest is the source of truth.
- During a backup run, a OneDrive seed built from the just-synced remote index trusts the manifest's resource list for collision state and skips the generic Lite data-directory reconcile. Manual verify/maintenance paths still use the generic directory listing and prune guards.
- A shared throttle gate fails new operations immediately until Graph's `Retry-After` deadline. Already accepted COPY and upload-session work waits for the gate and resumes instead of being abandoned.
- Lite manifest publish keeps the generic temp/backup/read-back safety model, but the OneDrive path uses item IDs returned by Graph for `tmp -> final`, `final -> bak`, read-back, and backup cleanup whenever the item was just observed by this client.
- Lite remote index sync is metadata-first. It lists `.watermelon/months`, compares size and modification time against the persisted snapshot digest, hydrates unchanged months locally, and downloads only changed manifest sqlite files. A missing or invalid local snapshot falls back to the normal full download.
- Graph delta is deliberately not wired into the first Personal release. If it is promoted later, the candidate scope should be the `.watermelon/months` folder item ID rather than the whole app-folder root, and it needs its own Personal live-validation matrix for token reset and deleted-item behavior.

References:

- [Upload session](https://learn.microsoft.com/en-us/graph/api/driveitem-createuploadsession?view=graph-rest-1.0)
- [Small file upload](https://learn.microsoft.com/en-us/graph/api/driveitem-put-content?view=graph-rest-1.0)
- [Working with files](https://learn.microsoft.com/en-us/graph/api/resources/onedrive?view=graph-rest-1.0)
- [Copy](https://learn.microsoft.com/en-us/graph/api/driveitem-copy?view=graph-rest-1.0)
- [Download content](https://learn.microsoft.com/en-us/graph/api/driveitem-get-content?view=graph-rest-1.0)
- [fileSystemInfo](https://learn.microsoft.com/en-us/graph/api/resources/filesysteminfo?view=graph-rest-1.0)
- [OneDrive and SharePoint filename restrictions](https://support.microsoft.com/en-us/onedrive/restrictions-and-limitations-in-onedrive-and-sharepoint)

## App registration

Register a public client in Microsoft Entra with:

- Supported account type: Personal Microsoft accounts only.
- iOS/macOS bundle ID: `com.zizicici.watermelon`.
- Redirect URI: `msauth.com.zizicici.watermelon://auth`.
- Delegated Microsoft Graph permission: `Files.ReadWrite.AppFolder`.
- Public client flow enabled.
- No client secret.

Put the public Application (client) ID in `OneDriveClientID` in `Watermelon/Resource/Info.plist`. The value is an application identifier, not a credential.

References:

- [Register an application](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app)
- [MSAL iOS redirect URIs](https://learn.microsoft.com/en-us/entra/msal/objc/redirect-uris-ios)

## Validation

Automated contract tests cover credential pinning and retention, opaque pagination, root-relative listing and upload addressing, lazy parent creation, conditional direct upload, conditional-create upload sessions, absence of `deferCommit`, resumable fragment throttling, bounded range recovery, finite-range shape independence, stall recovery, upload/monitor authorization boundaries, item-ID manifest publish/read-back, persisted manifest snapshot hydration, `202` COPY monitor states, post-completion verification, OneDrive-only filename rules, cross-origin redirect stripping, HTTPS downgrade rejection, and throttle behavior.

The add-destination flow performs a live write probe before save: create a temporary directory, conditionally create a file, prove a second conditional create collides, download and compare the winning content, and delete both artifacts.

Before release, run the following on a physical device with the Personal test account:

1. Sign in and confirm the app folder is created and the write probe passes.
2. Upload and download small, zero-byte, and multi-fragment files; stop and resume a multi-fragment backup.
3. Run two clients against the same lock path and confirm exactly one conditional create wins.
4. Exercise COPY success, destination replacement, monitor failure, and cancellation after `202 Accepted`.
5. Exercise move, delete/recycle-bin behavior, pagination, throttling, expired-token silent renewal, foreground reauthentication, and background silent-only behavior.
6. Rename or move the app folder and verify the pinned root item ID still reconnects; delete it and verify the profile fails closed instead of silently creating a new repository.

Business accounts require a separate permission, authority, drive-discovery, tenant-policy, and live-behavior review. They must not be enabled by widening the current account-type enum alone.
