import SwiftUI

struct LegacyScanResultListView: View {
    let report: LegacyScanReport
    let storageType: StorageType
    @State private var segment: ImportSegment = .all

    enum ImportSegment: Hashable {
        case all
        case toImport
        case alreadyInTarget
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            ScanSummaryHeader(report: report, storageType: storageType)
                .padding(.horizontal)
                .padding(.bottom, 6)

            segmentControl
                .padding(.horizontal)
                .padding(.bottom, 6)

            list
        }
    }

    private var segmentControl: some View {
        Picker("", selection: $segment) {
            Text(segmentLabel(.all, count: count(for: .all))).tag(ImportSegment.all)
            Text(segmentLabel(.toImport, count: count(for: .toImport))).tag(ImportSegment.toImport)
            Text(segmentLabel(.alreadyInTarget, count: count(for: .alreadyInTarget))).tag(ImportSegment.alreadyInTarget)
        }
        .pickerStyle(.segmented)
        .labelsHidden()
    }

    @ViewBuilder
    private var list: some View {
        let visible = report.plans
            .map { LegacyMonthPlan(id: $0.id, month: $0.month, bundles: $0.bundles.filter { matches($0.action, segment: segment) }) }
            .filter { !$0.bundles.isEmpty }

        let showUnscheduled = !report.unscheduledCandidates.isEmpty && segment != .toImport

        if visible.isEmpty && !showUnscheduled {
            ContentUnavailableView(
                String(localized: "migration.scan.empty.title"),
                systemImage: "tray",
                description: Text(String(localized: "migration.scan.empty.message"))
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else {
            List {
                ForEach(visible) { plan in
                    Section(header: monthHeader(for: plan)) {
                        ForEach(plan.bundles) { bundle in
                            BundleRow(bundle: bundle)
                        }
                    }
                }
                if showUnscheduled {
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
            Text(String(format: String(localized: "migration.scan.monthSummary.format"),
                        plan.totalAssetCount,
                        plan.totalResourceCount,
                        formatBytes(plan.totalFileSize)))
                .font(.caption)
                .foregroundStyle(.secondary)
            Spacer()
        }
    }

    private func count(for seg: ImportSegment) -> Int {
        report.plans.reduce(0) { acc, plan in
            acc + plan.bundles.lazy.filter { matches($0.action, segment: seg) }.count
        }
    }

    private func matches(_ action: LegacyBundleAction, segment: ImportSegment) -> Bool {
        switch segment {
        case .all: return true
        case .toImport:
            switch action {
            case .insertNew, .replacesSubsets: return true
            case .skipExactMatch, .skipEnclosed: return false
            }
        case .alreadyInTarget:
            switch action {
            case .skipExactMatch, .skipEnclosed: return true
            case .insertNew, .replacesSubsets: return false
            }
        }
    }

    private func segmentLabel(_ seg: ImportSegment, count: Int) -> String {
        let key: String.LocalizationValue
        switch seg {
        case .all: key = "migration.scan.segment.all"
        case .toImport: key = "migration.scan.segment.toImport"
        case .alreadyInTarget: key = "migration.scan.segment.alreadyInTarget"
        }
        return String(format: String(localized: key), count)
    }
}

private struct ScanSummaryHeader: View {
    let report: LegacyScanReport
    let storageType: StorageType

    private var totalBundles: Int { report.plans.reduce(0) { $0 + $1.totalAssetCount } }
    private var totalResources: Int { report.plans.reduce(0) { $0 + $1.totalResourceCount } }
    private var totalBytes: Int64 { report.plans.reduce(0) { $0 + $1.totalFileSize } }

    private var bytesToImport: Int64 {
        report.plans.reduce(0) { acc, plan in
            acc + plan.bundles.reduce(Int64(0)) { inner, bundle in
                switch bundle.action {
                case .insertNew, .replacesSubsets: return inner + bundle.totalFileSize
                case .skipExactMatch, .skipEnclosed: return inner
                }
            }
        }
    }

    private var estimatedSeconds: TimeInterval {
        let bytesPerSecond: Double
        switch storageType {
        case .externalVolume: bytesPerSecond = 60_000_000
        case .smb: bytesPerSecond = 12_000_000
        case .webdav: bytesPerSecond = 6_000_000
        }
        return Double(bytesToImport) / bytesPerSecond
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: 16) {
                summaryItem(String(localized: "migration.summary.months"), "\(report.plans.count)")
                summaryItem(String(localized: "migration.summary.bundles"), "\(totalBundles)")
                summaryItem(String(localized: "migration.summary.files"), "\(totalResources)")
                summaryItem(String(localized: "migration.summary.total"), formatBytes(totalBytes))
                if bytesToImport > 0 {
                    summaryItem(String(localized: "migration.summary.estimatedTime"), formatDuration(estimatedSeconds))
                }
            }
            if !report.warnings.isEmpty {
                ForEach(report.warnings.prefix(5), id: \.self) { warning in
                    Text(warning)
                        .font(.caption)
                        .foregroundStyle(.orange)
                        .lineLimit(2)
                        .truncationMode(.middle)
                }
                if report.warnings.count > 5 {
                    Text(String(format: String(localized: "migration.scan.moreWarnings.format"), report.warnings.count - 5))
                        .font(.caption)
                        .foregroundStyle(.secondary)
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

    private struct ActionStyle {
        let label: String
        let chipColor: Color
        let reason: String?
        let reasonColor: Color
    }

    private var actionStyle: ActionStyle {
        switch bundle.action {
        case .insertNew:
            return ActionStyle(
                label: String(localized: "migration.scan.action.new"),
                chipColor: .green,
                reason: nil,
                reasonColor: .secondary
            )
        case .skipExactMatch:
            return ActionStyle(
                label: String(localized: "migration.scan.action.skip"),
                chipColor: .gray,
                reason: String(localized: "migration.scan.reason.exactFingerprint"),
                reasonColor: .secondary
            )
        case .skipEnclosed:
            return ActionStyle(
                label: String(localized: "migration.scan.action.skip"),
                chipColor: .gray,
                reason: String(localized: "migration.scan.reason.enclosedAsset"),
                reasonColor: .secondary
            )
        case .replacesSubsets(let count):
            return ActionStyle(
                label: String(localized: "migration.scan.action.replacesOlder"),
                chipColor: .orange,
                reason: String(format: String(localized: "migration.scan.reason.subsetCount"), count),
                reasonColor: .orange
            )
        }
    }

    var body: some View {
        let style = actionStyle
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
                    if bundle.source == .manifest {
                        Text("M")
                            .font(.caption.bold())
                            .foregroundStyle(.purple)
                            .padding(.horizontal, 4)
                            .overlay(
                                RoundedRectangle(cornerRadius: 3)
                                    .stroke(Color.purple.opacity(0.6), lineWidth: 1)
                            )
                    }
                }
                .font(.caption)
                if let reason = style.reason {
                    Text(reason)
                        .font(.caption2)
                        .foregroundStyle(style.reasonColor)
                }
            }
            Spacer()
            chip(text: style.label, color: style.chipColor)
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
        return chip(text: label, color: color)
    }

    private var timestampSourceTag: some View {
        let label: String
        switch bundle.timestampSource {
        case .exif: label = "exif"
        case .quickTime: label = "qt"
        case .mtime: label = "mtime"
        case .unknown: label = "—"
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

    private func chip(text: String, color: Color) -> some View {
        Text(text)
            .font(.caption.bold())
            .foregroundStyle(.white)
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .background(color)
            .clipShape(Capsule())
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
