<div align="center">
  <img src="https://i.v2ex.co/T03Pw3rX.png" alt="Watermelon Backup app icon" width="118">
  <h1>Watermelon Backup</h1>
  <p><strong>Back up iPhone photos and videos to storage you control.</strong></p>
  <p>NAS, self-hosted servers, S3-compatible storage, WebDAV, SFTP, SMB, and external drives.</p>
  <p>
    <a href="https://apps.apple.com/app/id6762260596"><strong>Download on the App Store</strong></a>
    ·
    <a href="README.zh-CN.md">简体中文</a>
  </p>
  <a href="https://apps.apple.com/app/id6762260596">
    <img src="https://i.v2ex.co/wwG672a0.png" alt="Watermelon Backup screenshot" width="320">
  </a>
</div>

## Your Photos, Your Storage

Watermelon Backup helps you keep an independent copy of your iPhone photo library outside someone else's cloud. Point it at storage you already trust, choose the months you want to protect, and let it copy photos and videos directly from your device.

[Download Watermelon Backup on the App Store](https://apps.apple.com/app/id6762260596)

## What Makes It Useful

- Back up by month, so large libraries stay understandable.
- See local and remote status clearly before deciding what to upload, download, or sync.
- Use your own NAS, server, object storage bucket, SFTP folder, WebDAV directory, or external drive.
- Pause and resume long jobs without starting over.
- Restore backed-up photos and videos back into the Photos app.
- Handle iCloud Photos originals when a full backup needs the original file.
- Keep backup progress visible, with Picture in Picture progress available for Pro users.
- Enable background backup per storage profile with Pro.

## Supported Destinations

| Destination | Examples |
| --- | --- |
| SMB / NAS | Synology, QNAP, TrueNAS, Windows shares |
| WebDAV | Self-hosted WebDAV, compatible file servers |
| S3-compatible storage | S3-style object storage and private buckets |
| SFTP | Linux servers, VPS storage, SSH-based archives |
| External volume | Local disks and attached storage available to iOS |

## A Simple Backup Flow

1. Install Watermelon Backup from the App Store.
2. Allow Photos access.
3. Add your storage destination.
4. Select the months you want to protect.
5. Start upload, download, or sync.

Watermelon Backup uses a local index and remote manifests to avoid unnecessary repeat transfers whenever possible.

## Privacy

Watermelon Backup writes directly from your iPhone or iPad to the storage profile you configure. Credentials are stored through the system Keychain. There is no Watermelon-hosted cloud service in this repository.

## Download

- App Store: [https://apps.apple.com/app/id6762260596](https://apps.apple.com/app/id6762260596)
- App name: Watermelon Backup
- Category: Photo & Video

<details>
<summary>For developers</summary>

## Project Status

The iOS app is the primary product target in this repository.

`WatermelonMac` is a separate macOS target for legacy-data migration only. It has not been released as an App Store, TestFlight, or signed distribution build. Do not point it at irreplaceable photo libraries or production storage.

## Build From Source

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
