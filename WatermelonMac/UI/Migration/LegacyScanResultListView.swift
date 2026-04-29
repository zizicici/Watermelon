import SwiftUI

struct LegacyScanResultListView: View {
    let report: LegacyScanReport
    let storageType: StorageType

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            ScanSummaryHeader(report: report, storageType: storageType)
                .padding(.horizontal)
                .padding(.bottom, 6)
            List {
                ForEach(report.plans) { plan in
                    Section(header: monthHeader(for: plan)) {
                        ForEach(plan.bundles, id: \.id) { bundle in
                            BundleRow(bundle: bundle)
                        }
                    }
                }
                if !report.unscheduledCandidates.isEmpty {
                    Section(String(localized: "migration.skipped.section")) {
                        ForEach(report.unscheduledCandidates) { candidate in
                            HStack {
                                Image(systemName: "exclamationmark.triangle.fill")
                                    .foregroundStyle(.orange)
                                Text(candidate.originalFilename)
                                    .lineLimit(1)
                                    .truncationMode(.middle)
                                Spacer()
                                Text(formatBytes(candidate.fileSize))
                                    .foregroundStyle(.secondary)
                                    .font(.caption)
                            }
                        }
                    }
                }
            }
            .listStyle(.inset)
        }
    }

    private func monthHeader(for plan: LegacyMonthPlan) -> some View {
        HStack(spacing: 6) {
            Text(plan.month.text).font(.headline)
            Text("·").foregroundStyle(.secondary)
            Text("\(plan.totalAssetCount) bundles · \(plan.totalResourceCount) files · \(formatBytes(plan.totalFileSize))")
                .font(.caption)
                .foregroundStyle(.secondary)
            Spacer()
        }
    }
}

private struct ScanSummaryHeader: View {
    let report: LegacyScanReport
    let storageType: StorageType

    private var totalBundles: Int { report.plans.reduce(0) { $0 + $1.totalAssetCount } }
    private var totalResources: Int { report.plans.reduce(0) { $0 + $1.totalResourceCount } }
    private var totalBytes: Int64 { report.plans.reduce(0) { $0 + $1.totalFileSize } }

    private var estimatedSeconds: TimeInterval {
        let bytesPerSecond: Double
        switch storageType {
        case .externalVolume: bytesPerSecond = 60_000_000   // ~60 MB/s SSD-class
        case .smb: bytesPerSecond = 12_000_000              // ~12 MB/s typical home LAN
        case .webdav: bytesPerSecond = 6_000_000            // ~6 MB/s typical WebDAV
        }
        return Double(totalBytes) / bytesPerSecond
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: 16) {
                summaryItem(String(localized: "migration.summary.months"), "\(report.plans.count)")
                summaryItem(String(localized: "migration.summary.bundles"), "\(totalBundles)")
                summaryItem(String(localized: "migration.summary.files"), "\(totalResources)")
                summaryItem(String(localized: "migration.summary.total"), formatBytes(totalBytes))
                if totalBytes > 0 {
                    summaryItem(String(localized: "migration.summary.estimatedTime"), formatDuration(estimatedSeconds))
                }
            }
            if !report.warnings.isEmpty {
                ForEach(report.warnings, id: \.self) { warning in
                    Text(warning)
                        .font(.caption)
                        .foregroundStyle(.orange)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    @ViewBuilder
    private func summaryItem(_ label: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 0) {
            Text(label).font(.caption).foregroundStyle(.secondary)
            Text(value).font(.body.monospacedDigit())
        }
    }
}

private func formatDuration(_ seconds: TimeInterval) -> String {
    if seconds < 5 { return "<5s" }
    if seconds < 60 { return "\(Int(seconds))s" }
    if seconds < 3600 {
        let m = Int(seconds / 60)
        let s = Int(seconds.truncatingRemainder(dividingBy: 60))
        return s == 0 ? "\(m)m" : "\(m)m \(s)s"
    }
    let h = Int(seconds / 3600)
    let m = Int((seconds.truncatingRemainder(dividingBy: 3600)) / 60)
    return m == 0 ? "\(h)h" : "\(h)h \(m)m"
}

private struct BundleRow: View {
    let bundle: LegacyAssetBundle

    var body: some View {
        HStack(alignment: .top, spacing: 10) {
            kindBadge
            VStack(alignment: .leading, spacing: 2) {
                Text(bundle.resources.first?.originalFilename ?? "(unknown)")
                    .font(.body.monospaced())
                    .lineLimit(1)
                    .truncationMode(.middle)
                HStack(spacing: 8) {
                    if let date = bundle.creationDate {
                        Text(formatDate(date)).foregroundStyle(.secondary)
                    }
                    timestampSourceTag
                    Text("fp:\(bundle.assetFingerprint.hexString.prefix(8))")
                        .foregroundStyle(.secondary)
                    Text(formatBytes(bundle.totalFileSize))
                        .foregroundStyle(.secondary)
                }
                .font(.caption)
            }
            Spacer()
        }
        .padding(.vertical, 2)
    }

    private var kindBadge: some View {
        let label: String
        let color: Color
        switch bundle.kind {
        case .livePhoto: label = "LIVE"; color = .purple
        case .photo: label = "PHOTO"; color = .blue
        case .video: label = "VIDEO"; color = .orange
        }
        return Text(label)
            .font(.caption.bold())
            .foregroundStyle(.white)
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .background(color)
            .clipShape(Capsule())
    }

    private var timestampSourceTag: some View {
        let label: String
        switch bundle.timestampSource {
        case .exif: label = "exif"
        case .quickTime: label = "qt"
        case .mtime: label = "mtime"
        case .unknown: label = "?"
        }
        return Text(label)
            .font(.caption.smallCaps())
            .padding(.horizontal, 6)
            .padding(.vertical, 1)
            .overlay(
                RoundedRectangle(cornerRadius: 4)
                    .stroke(Color.secondary.opacity(0.5), lineWidth: 1)
            )
            .foregroundStyle(.secondary)
    }
}

private let dateFormatter: DateFormatter = {
    let f = DateFormatter()
    f.dateFormat = "yyyy-MM-dd HH:mm"
    return f
}()

private func formatDate(_ date: Date) -> String {
    dateFormatter.string(from: date)
}

private func formatBytes(_ bytes: Int64) -> String {
    ByteCountFormatter.fileSizeString(bytes)
}
