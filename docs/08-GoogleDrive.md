# Google Drive

## Scope

Google Drive nodes use a user-supplied iOS OAuth client ID. Watermelon never ships a shared Google project for this backend and never requires a client secret. The user enables Drive API, configures the OAuth audience and test user, creates an iOS OAuth client for `com.zizicici.watermelon`, and enters its public client ID in the node editor. External projects intended for lasting background access must be published to Production; Testing refresh tokens can expire after seven days. App Check enforcement must remain disabled because a user-owned Firebase project cannot attest this App Store binary.

Authorization uses OAuth authorization code + PKCE through `ASWebAuthenticationSession`. The requested scopes are `openid`, `email`, `https://www.googleapis.com/auth/drive.file`, and `https://www.googleapis.com/auth/drive.appdata`. The refresh token is stored in Keychain; access tokens remain in memory. `drive.file` limits the visible repository to files created by the user's Google app, while `drive.appdata` isolates the lock control plane from normal Drive file management.

Each Google app/account pair owns one app-created `Watermelon Backup` root folder in My Drive. The profile pins the Google account subject, root folder ID, and the first repository lock slot ID. All normal paths are resolved relative to the pinned root ID, so a user rename or move does not silently redirect the profile.

Google Drive is a V2-only node. Its app-created root has no Repo V1 history: an empty root is initialized directly as V2, and V1-shaped evidence is treated as damaged/foreign data rather than routed into the legacy migration engine. The macOS legacy importer does not expose Google Drive profiles.

## End-user Google Cloud setup

This section is the source of truth for the future public setup page linked below the OAuth client ID field. The in-app entry should open the public version of this guide rather than a single Cloud Console page, because setup spans several Google Auth Platform pages. Suggested link text: `How to get an OAuth client ID` / `如何获取 OAuth 客户端 ID`. The public page should retain the direct Console links below.

### Before starting

The user needs:

- a Google account whose Drive will hold the backup;
- permission to create or select a Google Cloud project;
- Watermelon for iOS, whose bundle ID is exactly `com.zizicici.watermelon`;
- about five minutes for a Testing configuration;
- for long-lived External/Production access, a public HTTPS homepage and privacy policy on a domain the project owner owns and can verify.

No paid Google Cloud service, API key, service account, client secret, downloaded `credentials.json`, Firebase project, or manually created Drive folder is part of this flow. Google may ask for billing in some Cloud environments, but Watermelon itself does not use a billable Google Cloud resource or a server-side credential.

### 1. Create or select a Google Cloud project

Open [Google Cloud Console: New project](https://console.cloud.google.com/projectcreate). A dedicated project is recommended so its OAuth audience, quota, and credentials are not mixed with another application.

1. Enter a project name such as `Watermelon Backup`.
2. Personal Google accounts can leave the organization/location selection at its default. A managed Google Workspace account may require an organization or folder selected by its administrator.
3. Google generates a globally unique project ID. The generated value is fine; Watermelon never asks for it. The project ID cannot be changed after project creation.
4. Select `Create`, wait for creation to finish, and make sure the new project is selected in the project picker before continuing.

Creating a project requires Google's `resourcemanager.projects.create` permission. If a managed account cannot create one, its Workspace/Cloud administrator must create it or grant that permission. See [Create projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects#creating_a_project).

### 2. Enable Google Drive API

Open [Google Drive API in the API Library](https://console.cloud.google.com/apis/library/drive.googleapis.com), verify the project selector still shows the project from step 1, and select `Enable`.

Only **Google Drive API** (`drive.googleapis.com`) is required. Drive Activity API, Drive Labels API, Google Picker API, an API key, and a service account are not required. If `drive.file` or `drive.appdata` is missing later in the scope picker, return here and confirm that Google Drive API is enabled. See [Enable Google Workspace APIs](https://developers.google.com/workspace/guides/enable-apis).

### 3. Register the app in Google Auth Platform

Open [Google Auth Platform: Branding](https://console.cloud.google.com/auth/branding). If the project has not been registered for Google Auth, select `Get started` and complete the wizard:

1. **App name:** use a recognizable private name such as `Watermelon Backup`.
2. **User support email:** select an email address the project owner can receive.
3. **Audience:** choose using step 4 below.
4. **Contact information:** enter an email address that should receive OAuth policy, client deletion, and project notifications.
5. Review and accept the Google API Services User Data Policy, then create the app registration.

An app logo is unnecessary for a personal project. Adding one can trigger brand verification. Before publishing an External app to Production, complete the App Domain section even though Watermelon uses only non-sensitive scopes:

1. Host a public HTTPS homepage on a domain owned by the project owner. It must identify the OAuth app, explain that it provides a personal Google Drive connection for Watermelon, and not be only a sign-in page.
2. Publish a privacy policy on that domain, link it from the homepage, and explain how the OAuth app accesses, stores, and uses Google user data.
3. Enter the same homepage and privacy-policy URLs in Branding. Terms of service are optional unless Google specifically requests them.
4. Add the registrable domain under Authorized domains and verify ownership with the same Google account in [Google Search Console](https://search.google.com/search-console/about).

These domain and public-page requirements apply to External production apps; they are separate from sensitive/restricted-scope verification. See [Configure OAuth consent](https://developers.google.com/workspace/guides/configure-oauth-consent), [Manage OAuth app branding](https://support.google.com/cloud/answer/15549049?hl=en), and [OAuth production policy compliance](https://developers.google.com/identity/protocols/oauth2/production-readiness/policy-compliance).

### 4. Choose the audience and publishing status

Open [Google Auth Platform: Audience](https://console.cloud.google.com/auth/audience).

- Choose **Internal** only when the project belongs to a Google Workspace organization and every account that will use this node belongs to that organization. Internal is not available to ordinary personal Gmail projects.
- Otherwise choose **External**. While the app is in `Testing`, add the exact Google account used for the backup under `Test users`. Add every additional account that needs to authorize this client.

Testing is suitable only for initial setup. For an External app requesting Drive scopes, a refresh token issued while the publishing status is `Testing` expires after seven days. Before relying on background backup:

1. Complete the External production Branding and verified-domain requirements in step 3.
2. On the Audience page, select `Publish app` or the equivalent action that changes the publishing status to `In production`.
3. Confirm the status displayed by Google is `In production`.
4. If Watermelon was authorized while the project was still in Testing, reconnect the Google Drive node once after publishing. Do not assume an already-issued seven-day token becomes long-lived.

Publishing status and OAuth verification are separate concepts. Watermelon's exact Drive scopes are non-sensitive, so they do not require sensitive/restricted-scope justification or a third-party security assessment; this does not exempt an External production app from Google's public homepage, privacy policy, authorized-domain, or brand requirements. A personal project should keep its scope set and branding minimal instead of adding broader permissions to work around a Console prompt. See [Manage app audience](https://support.google.com/cloud/answer/15549945?hl=en) and Google's [refresh-token expiration rules](https://developers.google.com/identity/protocols/oauth2#expiration).

### 5. Declare the exact OAuth scopes

Open [Google Auth Platform: Data Access](https://console.cloud.google.com/auth/scopes), select `Add or remove scopes`, and configure the following scopes. If a scope is not offered by the picker, use `Manually add scopes` and paste its full value.

| Data Access value | Why Watermelon needs it |
| --- | --- |
| `openid` | Reads the stable Google account subject used to pin the saved node to one account. |
| `https://www.googleapis.com/auth/userinfo.email` | Displays the authorized account to the user. It does not grant Gmail access. Watermelon requests the equivalent OpenID Connect shorthand `email`; Google may already list this identity scope by default. |
| `https://www.googleapis.com/auth/drive.file` | Creates and manages only Drive files associated with this user-owned OAuth app, including the `Watermelon Backup` repository. |
| `https://www.googleapis.com/auth/drive.appdata` | Stores the small hidden lock-control records in the app's private Drive App Data area. |

Select `Update`/`Save` and confirm the scopes appear under the configured non-sensitive scopes. Do **not** add `https://www.googleapis.com/auth/drive`, `drive.readonly`, or any other whole-Drive scope. Google classifies both `drive.file` and `drive.appdata` as non-sensitive scopes recommended for narrow app access. See [Choose Google Drive API scopes](https://developers.google.com/workspace/drive/api/guides/api-specific-auth) and [Manage app data access](https://support.google.com/cloud/answer/15549135?hl=en).

### 6. Create the iOS OAuth client

Open [Google Auth Platform: Clients](https://console.cloud.google.com/auth/clients) and select `Create client`.

1. **Application type:** choose `iOS`. Do not choose Web application, Desktop app, Android, or TV/limited input.
2. **Name:** use a Console-only name such as `Watermelon iOS`.
3. **Bundle ID:** enter exactly `com.zizicici.watermelon`. This value is case-sensitive and must not contain a wildcard.
4. **App Store ID:** optional; leave it blank unless the public Watermelon App Store ID is explicitly provided by the setup guide.
5. **Team ID:** optional for this flow; leave it blank. It is required by Google only when configuring iOS App Check.
6. Select `Create`.
7. Open the created client's edit page. If Google shows `Protect your OAuth client from abuse with Firebase App Check`, leave it disabled. On an existing client, make sure App Check is not enforced; Watermelon cannot produce App Check tokens for a Firebase project owned by the user.

The result must be an OAuth client ID ending in `.apps.googleusercontent.com`, for example:

```text
123456789012-abcdefghijklmnopqrstuvwxyz.apps.googleusercontent.com
```

Copy only the **Client ID**. It is the public identifier Watermelon asks for. An iOS/native app is a public OAuth client and cannot safely keep a secret, so Watermelon never asks for or sends a client secret. See [Create an iOS OAuth client](https://developers.google.com/workspace/guides/create-credentials#ios) and [Manage OAuth clients](https://support.google.com/cloud/answer/15549257?hl=en).

Keep this Client ID with the node configuration. A second device, a reinstall, or a restored Watermelon database must reuse the same Google Cloud project, the same iOS Client ID, and the same Google account to reconnect to the same repository identity. Creating another project or Client ID can be treated as a separate app identity by Drive and Watermelon and can lead to a separate `Watermelon Backup` repository.

Watermelon derives the callback from the client ID. For the example above it uses:

```text
com.googleusercontent.apps.123456789012-abcdefghijklmnopqrstuvwxyz:/oauth2redirect
```

There is no authorized-redirect-URI field to fill in for an iOS client, and no redirect URL should be added to a Web client. Google supports a reversed client ID custom scheme with an optional `/oauth2redirect` path for installed apps. Watermelon performs authorization code exchange with PKCE (`S256`) through the system authentication session. See [OAuth 2.0 for iOS and desktop apps](https://developers.google.com/identity/protocols/oauth2/native-app).

### 7. Connect the node in Watermelon

1. In Watermelon, add a Google Drive node.
2. Paste the full Client ID into `OAuth client ID`; do not paste a project ID, numeric project number, client secret, or JSON configuration.
3. Start authorization and sign in with an account allowed by the Audience configuration. For External/Testing, it must be listed as a test user.
4. Review the Google consent screen and approve all requested access; do not deselect either Drive permission.
5. Return to Watermelon and wait while it creates or reconciles the repository and then verifies write access.
6. Save the node after the account and folder are shown.

Watermelon creates `Watermelon Backup` in My Drive automatically. Do not create that folder manually. The tiny append-only lock records live in hidden Drive App Data and are not ordinary files in My Drive. Watermelon stores the refresh token in iOS Keychain and keeps access tokens only in memory.

### Troubleshooting setup

| Symptom | Check |
| --- | --- |
| `Access blocked`, `access_denied`, or the account is not allowed | For External/Testing, add the exact account under Audience > Test users. On managed Workspace accounts, an administrator may also need to allow the OAuth client. |
| `redirect_uri_mismatch` or authorization does not return to Watermelon | Confirm the client type is iOS, the bundle ID is exactly `com.zizicici.watermelon`, and the pasted value is that iOS client's ID rather than a Web/Desktop client ID. |
| Drive returns `403`, `accessNotConfigured`, or says the API is disabled | Select the same Cloud project and enable Google Drive API. Confirm `drive.file`, `drive.appdata`, and the identity scopes are configured under Data Access. |
| Authorization works in Testing but the node asks to sign in again about seven days later | Publish the External app to `In production`, then reconnect once to issue a new refresh token. |
| OAuth fails after App Check enforcement was enabled | Select `UNENFORCE` or disable App Check for the iOS client. Enabling monitoring alone does not block OAuth; Google notes that enforcement changes can take up to 15 minutes to propagate. |
| Google reports `deleted_client` or `invalid_client` | The OAuth client was deleted, copied incorrectly, or belongs to another application type. Restore it within Google's recovery window or create a new iOS client and reconnect the node. |
| OAuth succeeds but Watermelon reports that storage configuration is unavailable | Keep the Drive API and requested permissions enabled, make sure the authorized account has usable Drive storage, and retry setup. Watermelon creates and reconciles its own root; do not create or rename control files to work around the error. |

Users can revoke the project from [Google Account third-party access](https://myaccount.google.com/permissions). Revocation invalidates the saved refresh token, so the Watermelon node must be authorized again afterward. Google can also delete an OAuth client after six months with neither an OAuth credential/token request nor a client configuration change; the project contact email receives 30 days' advance notice. Normal Watermelon token refreshes count as token requests.

## File behavior

- Drive names are not unique within a folder. Path resolution therefore fails closed when more than one visible child has the requested name.
- Directory creation is idempotent only when exactly one existing child is a folder. Clients for one pinned root share a process-wide directory mutation gate, and an indeterminate create is checked once by its generated ID. Under an active write lease, a data or `.watermelon` directory proven absent is installed as an empty snapshot from its validated create response; only a pre-existing unloaded target needs one child listing before uploads. Root creation, and `.watermelon` creation without authoritative leased absence, perform bounded delayed observations before writing children, choose the oldest candidate, and remove every confirmed-empty loser. Root discovery consumes all result pages; the bootstrap settles only after creation or duplicate discovery, while an existing unique root avoids the delayed observation loop. A nonempty loser fails closed.
- Ordinary files, copies, and directories use pre-generated file IDs fetched in small batches. A fixed ID lets the current operation check an indeterminate result without allocating one ID per request. Root bootstrap and the append-only lock chain allocate their small ID groups directly in their respective `drive` and `appDataFolder` spaces because those IDs are persisted as control state.
- Outside a write lease, replace updates the freshly resolved file ID in place and create-if-absent performs exact-name preflight plus a generated-ID winner check. During either a foreground or background write lease, clients for the pinned root share one directory snapshot whose entries are `present`, `uploading`, or `uncertain`. A validated successful response becomes `present` immediately and adds no confirmation LIST. An indeterminate upload keeps its fixed ID in the current session; a retry of that path or the next authoritative directory listing can accept the matching object or restore an unchanged prior object. Unconfirmed ordinary files remain recoverable orphans and are never referenced by a manifest. This is sufficient after the repository lock is held, but it is not the write-lock primitive because two independent namesake creates can both succeed in Drive.
- List consumes every `nextPageToken` and rejects duplicate or unsafe path-component names before caching. Download uses `alt=media`; outside a lease it reuses cached ancestor IDs but resolves the leaf name on every transfer, so sibling restores avoid repeated path walks without trusting a stale file ID. Leased same-directory move and delete use the authoritative snapshot directly, while strict or cross-directory mutations resolve the live parent. Unknown MOVE and copy results are reconciled by their fixed destination file IDs. Delete permanently removes the app-owned item so deleted backups stop consuming the user's quota.
- The leased write session starts immediately after the repository lock is acquired, so the owned format probe and subsequent month loads share one namespace snapshot. It distinguishes an unloaded directory from a child proven absent, so `metadata` and `exists` do not repeat path resolution for known misses. After `.watermelon` is observed, or its leased absence is followed by a validated creation, its children join the same snapshot and version upload, move, and read-back do not replay the path from the root. When the owned probe proves that `.watermelon` did not exist before a fresh version commit, the transition skips the post-commit orphan scan because no older control scratch can exist. Downloads use the listed file ID directly and fall back to fresh full-path resolution after a stale-ID 404. Ordinary operations do not consult the client's strict path cache while a session snapshot is authoritative. The logical `.watermelon/locks` directory is virtual; its Google-only control objects live directly in `appDataFolder` and keep their strict append-only proof flow.
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
2. A lock record is created in `appDataFolder` at the currently unused slot ID. Its immutable envelope contains the virtual `.watermelon/locks/<writer>.lock` path, the normal `LockFileBody`, a newly generated next-slot ID, and a newly generated release-marker ID. App properties bind every record and release marker to the visible repository root ID.
3. Contenders create the same current slot ID. Exactly one succeeds; the others receive `409` and observe the winner.
4. Refresh updates only the winning record and first verifies that the embedded session and lock tokens still match.
5. Release creates the record's fixed release-marker ID. It never deletes or reuses the record ID.
6. Only the highest-sequence record is operational state. A matching release marker advances the cursor; otherwise the normal lock service applies its freshness window before blocking or taking over. Missing older history is ignored.
7. The next acquisition follows the released record to its next-slot ID. A stale record is still exposed to `WriteLockService`, whose existing double-read takeover rules create its release marker before continuing.

`GoogleDriveClient` virtualizes the current chain record as the ordinary lock path for `list`, `metadata`, `download`, `upload`, `setModificationDate`, and `delete`. The generic `WriteLockService` remains the authority for freshness, ownership proof, refresh cadence, stale takeover, and release policy.

Foreground Google Drive writers may reuse the generic service's bounded recent lease confidence for recoverable writes. Background writers may reuse it only for ordinary immutable `createIfAbsent` asset uploads; manifest and other control writes still prove ownership remotely. Confidence expiry falls back to a remote proof, and destructive writes may reuse confidence only for an attended lease. Lock cursor scans reuse the complete metadata returned by the folder listing and download only the active record body; fixed-ID recovery paths still fetch metadata by ID.

Profile setup also acquires and releases a deterministic, scoped `WriteLockService` lease. It confirms through the logical lock cursor that release is remotely visible before returning a saveable profile. A canceled setup can therefore be recovered immediately by the next setup from the same app/account/root, and an indeterminate slot create is reconciled by reading the fixed generated slot ID before deciding its outcome.

Record sequence, next-slot, and release-marker IDs are duplicated into private app properties. A single paginated folder listing can therefore select the latest record without downloading old bodies; only the active record body is fetched and verified. This keeps acquisition request count bounded by listing pages as the append-only history grows.

Old lock records and release markers are retained in hidden App Data because Drive file IDs cannot be reused. They are tiny control objects and never contain OAuth material or photo data. Deleting some old records does not invalidate the node. If Watermelon's App Data is completely empty during profile setup, the existing visible repository receives a fresh first-slot ID and locking starts over; ordinary Drive file deletion cannot expose these objects.

## Validation

Automated tests must cover credential validation, OAuth URL/redirect derivation, pagination, duplicate-name rejection, upload retry by generated ID, account pinning, and the virtual lock chain's acquire/collision/refresh/release/stale-takeover behavior.

Before release, validate on a physical device with a user-created Google app:

1. first authorization, refresh after access-token expiry, revoked consent, and account switching;
2. discovery of the existing app-created root on a second device using the same client ID;
3. small and resumable uploads, stop/resume, download, move, copy, and permanent delete;
4. two devices acquiring the same repository concurrently, including an expired holder and a refresh/takeover race;
5. quota exhaustion, 401 retry, 403 rate limits, 429/5xx backoff, offline recovery, and ambiguous duplicate names.
