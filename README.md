<div align="center">
  <img src="https://i.v2ex.co/T03Pw3rXb.png" alt="Watermelon Backup app icon" width="120">
  <h1>Watermelon Backup</h1>
  <p><strong>Backup iPhone Photos to Your Storage</strong></p>
  <p>Supports Backup to External Storage, SMB, WebDAV, S3-Compatible Buckets, and SFTP.<br>Open Source on GitHub.</p>
  <p>
    <a href="https://apps.apple.com/app/id6762260596"><strong>Get for FREE</strong></a>
    ·
    <a href="https://watermelonbackup.com/">Official Website</a>
    ·
    <a href="README.zh-CN.md">简体中文</a>
  </p>
  <a href="https://apps.apple.com/app/id6762260596">
    <img src="https://i.v2ex.co/2U0JtN6N.png" alt="Watermelon home screen showing nodes and month tasks" width="320">
  </a>
</div>

## Easy to Use

| Step | What happens |
| --- | --- |
| Choose a Node | External Storage, SMB, WebDAV, S3-Compatible Buckets, or SFTP. |
| Check Months | Backup, Download, or Complement by month. |
| Run | Sync the Remote Index, Then Pause or Resume When Needed. |

## Backup, Download, Complement

| Action | Meaning |
| --- | --- |
| Backup | Writes original photos and videos from this device to the current Node. |
| Download | Restores backed-up items from the Node into Photos. |
| Complement | Backs up first, then downloads: it sends local items missing on the Node, then brings remote-only items back into Photos. |

## Open Source and Reviewable

We believe privacy matters as much as ownership.

- Source Code: [github.com/zizicici/Watermelon](https://github.com/zizicici/Watermelon)
- Privacy Policy: [watermelonbackup.com/privacy.html](https://watermelonbackup.com/privacy.html)
- Official Website: [watermelonbackup.com](https://watermelonbackup.com/)

## Pricing

One-Time Purchase. No Subscription.

| Item | Free | Pro |
| --- | --- | --- |
| Price | - | US$7.99, About 2 Coffees |
| Foreground Backup | Included | Included |
| Node(s) | 1 | Unlimited |
| Automatic Background Backup | Not included | Included |
| PiP Progress | Not included | Included |
| Focus Mode | Not included | Included |

Pro is a one-time purchase and stays available for life. On a new device, use Restore Purchase to restore your entitlement.

## FAQ Highlights

- A Node is a storage connection you add in the app. It can be External Storage, SMB, WebDAV, an S3-Compatible Bucket, or SFTP.
- Credentials required by Nodes are stored in iOS Keychain and are not uploaded to Watermelon Backup servers.
- Watermelon Backup can back up and restore Live Photos by saving each Live Photo as a still image and paired video, then merging it back when imported.
- Photos and videos are not recompressed. Watermelon Backup keeps the original files as much as possible, so capture time and EXIF stay with the originals.
- If iCloud Photos is enabled, turn on Allow iCloud Photo Access so Watermelon Backup can fetch iCloud originals when needed.

## Download

- App Store: [https://apps.apple.com/app/id6762260596](https://apps.apple.com/app/id6762260596)
- Official Website: [https://watermelonbackup.com](https://watermelonbackup.com/)
- App name: Watermelon Backup

<details>
<summary>For developers</summary>

## Project Status

The iOS app is the primary product target in this repository.

`WatermelonMac` is a separate macOS target for legacy-data migration only. It has not been released as an App Store, TestFlight, or signed distribution build. Do not point it at irreplaceable photo libraries or production storage.

## Build From Source

Watermelon Backup is open source, and you can compile the app directly in Xcode.

1. Open `Watermelon.xcodeproj` in Xcode.
2. Select the `Watermelon` scheme for the iOS app.
3. Run on a simulator or a real device.
4. Run the `WatermelonTests` target for the included unit tests.

## Repository Map

| Path | Purpose |
| --- | --- |
| `Watermelon/` | iOS app source: Home, onboarding, settings, backup orchestration, PhotoKit integration |
| `Shared/` | Shared storage clients, database, Keychain, domain models, manifests, repo services |
| `WatermelonMac/` | macOS legacy migration target; not the iOS backup pipeline |
| `WatermelonTests/` | XCTest coverage for pure logic, storage signing, credentials, write-lock, and cleanup behavior |
| `docs/` | Architecture, backup pipeline, data model, UI flow, and known technical issues |

## Technical Documentation

- `AGENTS.md` - concise project guide for coding agents
- `docs/01-Architecture.md` - module layering and dependencies
- `docs/02-BackupCoreV2.md` - upload, sync, download, preflight, and retry details
- `docs/03-DataModel.md` - SQLite schemas and snapshot models
- `docs/04-UIFlow.md` - Home, connection, onboarding, More page, and execution states
- `docs/05-OpenIssues.md` - current risks and technical debt

</details>
