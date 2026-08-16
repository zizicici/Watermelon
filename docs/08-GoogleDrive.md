# Google Drive

## Scope

Google Drive nodes use a user-supplied iOS OAuth client ID. Watermelon never ships a shared Google project for this backend and never requires a client secret. The user enables Drive API, configures the OAuth audience and test user, creates an iOS OAuth client for `com.zizicici.watermelon`, and enters its public client ID in the node editor. External projects intended for lasting background access must be published to Production; Testing refresh tokens can expire after seven days. App Check enforcement must remain disabled because a user-owned Firebase project cannot attest this App Store binary.

Authorization uses OAuth authorization code + PKCE through `ASWebAuthenticationSession`. The requested scopes are `openid`, `email`, and `https://www.googleapis.com/auth/drive.file`. The refresh token is stored in Keychain; access tokens remain in memory. `drive.file` limits the node to files created by the user's Google app.

Each Google app/account pair owns one app-created `Watermelon` root folder in My Drive. The profile pins the Google account subject, root folder ID, and the first repository lock slot ID. All normal paths are resolved relative to the pinned root ID, so a user rename or move does not silently redirect the profile.

Google Drive is a V2-only node. Its app-created root has no Repo V1 history: an empty root is initialized directly as V2, and V1-shaped evidence is treated as damaged/foreign data rather than routed into the legacy migration engine. The macOS legacy importer does not expose Google Drive profiles.

References:

- [Choose Google Drive API scopes](https://developers.google.com/workspace/drive/api/guides/api-specific-auth)
- [OAuth 2.0 for iOS and desktop apps](https://developers.google.com/identity/protocols/oauth2/native-app)
- [Create and manage files](https://developers.google.com/workspace/drive/api/guides/create-file)

## File behavior

- Drive names are not unique within a folder. Path resolution therefore fails closed when more than one visible child has the requested name.
- Directory creation is idempotent only when exactly one existing child is a folder. Clients for one pinned root share a process-wide directory mutation gate, and an indeterminate create is checked once by its generated ID. Under an active write lease, a normal data directory proven absent is installed as an empty snapshot from its validated create response; only a pre-existing unloaded target needs one child listing before uploads. Root and lock-control creation instead perform bounded delayed observations before writing children, choose the oldest candidate, and remove every confirmed-empty loser. Root discovery consumes all result pages; the bootstrap settles only after creation or duplicate discovery, while an existing unique root avoids the delayed observation loop. A nonempty loser fails closed.
- Ordinary files, copies, and directories use pre-generated file IDs fetched in small batches. A fixed ID lets the current operation check an indeterminate result without allocating one ID per request. Root bootstrap and the append-only lock chain allocate their small ID groups directly because those IDs are persisted as control state.
- Outside a write lease, replace updates the freshly resolved file ID in place and create-if-absent performs exact-name preflight plus a generated-ID winner check. During either a foreground or background write lease, clients for the pinned root share one directory snapshot whose entries are `present`, `uploading`, or `uncertain`. A validated successful response becomes `present` immediately and adds no confirmation LIST. An indeterminate upload keeps its fixed ID in the current session; a retry of that path or the next authoritative directory listing can accept the matching object or restore an unchanged prior object. Unconfirmed ordinary files remain recoverable orphans and are never referenced by a manifest. This is sufficient after the repository lock is held, but it is not the write-lock primitive because two independent namesake creates can both succeed in Drive.
- List consumes every `nextPageToken` and rejects duplicate or unsafe path-component names before caching. Download uses `alt=media`; outside a lease it reuses cached ancestor IDs but resolves the leaf name on every transfer, so sibling restores avoid repeated path walks without trusting a stale file ID. Leased same-directory move and delete use the authoritative snapshot directly, while strict or cross-directory mutations resolve the live parent. Unknown MOVE and copy results are reconciled by their fixed destination file IDs. Delete permanently removes the app-owned item so deleted backups stop consuming the user's quota.
- The leased write session distinguishes an unloaded directory from a child proven absent, so `metadata` and `exists` do not repeat path resolution for known misses. Downloads use the listed file ID directly and fall back to fresh full-path resolution after a stale-ID 404. Ordinary operations do not consult the client's strict path cache while a session snapshot is authoritative. Lock control directories are excluded from the snapshot and keep their strict append-only proof flow.
- Upload metadata includes the local temporary file's modification time, so the backup pipeline does not issue a second metadata-only PATCH after a successful upload.
- Files larger than 5 MiB use 8 MiB resumable chunks. After an unknown chunk result the client queries the session's received range and continues from the confirmed offset; a 404-expired session is restarted by the outer upload retry.

## Strict write lock

Drive permits duplicate names and a used pre-generated file ID cannot be recreated after permanent deletion. Simulator validation on 2026-08-15 established:

- two concurrent creates with one generated ID returned one success and one `409`;
- the successful body read back unchanged;
- permanent delete returned `204`;
- recreating the deleted ID returned `400`.

Google Drive therefore uses an append-only lock slot chain rather than a reusable lock filename:

1. The root folder stores a generated first slot ID in its private app properties and the profile pins the same value.
2. A lock record is created at the currently unused slot ID. Its immutable envelope contains the virtual `.watermelon/locks/<writer>.lock` path, the normal `LockFileBody`, a newly generated next-slot ID, and a newly generated release-marker ID.
3. Contenders create the same current slot ID. Exactly one succeeds; the others receive `409` and observe the winner.
4. Refresh updates only the winning record and first verifies that the embedded session and lock tokens still match.
5. Release creates the record's fixed release-marker ID. It never deletes or reuses the record ID.
6. The next acquisition follows the released record to its next-slot ID. A stale record is still exposed to `WriteLockService`, whose existing double-read takeover rules create its release marker before continuing.

`GoogleDriveClient` virtualizes the current chain record as the ordinary lock path for `list`, `metadata`, `download`, `upload`, `setModificationDate`, and `delete`. The generic `WriteLockService` remains the authority for freshness, ownership proof, refresh cadence, stale takeover, and release policy.

Foreground Google Drive writers may reuse the generic service's bounded recent lease confidence for recoverable writes. Background writers may reuse it only for ordinary immutable `createIfAbsent` asset uploads; manifest and other control writes still prove ownership remotely. Confidence expiry falls back to a remote proof, and destructive writes may reuse confidence only for an attended lease. Lock cursor scans reuse the complete metadata returned by the folder listing and download only the active record body; fixed-ID recovery paths still fetch metadata by ID.

Profile setup also acquires and releases a deterministic, scoped `WriteLockService` lease. It confirms through the logical lock cursor that release is remotely visible before returning a saveable profile. A canceled setup can therefore be recovered immediately by the next setup from the same app/account/root, and an indeterminate slot create is reconciled by reading the fixed generated slot ID before deciding its outcome.

Record sequence, next-slot, and release-marker IDs are duplicated into private app properties. A single paginated folder listing can therefore traverse all released history without downloading every old body; only the active record body is fetched and verified. This keeps acquisition request count bounded by listing pages as the append-only history grows.

Old lock records and release markers are deliberately retained: they are the immutable proof chain. They are tiny control objects and never contain OAuth material or photo data.

## Validation

Automated tests must cover credential validation, OAuth URL/redirect derivation, pagination, duplicate-name rejection, upload retry by generated ID, account pinning, and the virtual lock chain's acquire/collision/refresh/release/stale-takeover behavior.

Before release, validate on a physical device with a user-created Google app:

1. first authorization, refresh after access-token expiry, revoked consent, and account switching;
2. discovery of the existing app-created root on a second device using the same client ID;
3. small and resumable uploads, stop/resume, download, move, copy, and permanent delete;
4. two devices acquiring the same repository concurrently, including an expired holder and a refresh/takeover race;
5. quota exhaustion, 401 retry, 403 rate limits, 429/5xx backoff, offline recovery, and ambiguous duplicate names.
