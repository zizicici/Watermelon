import AppKit

@MainActor
final class MacLegacyScanResultViewController: NSViewController {
    private enum ImportSegment: Int {
        case all
        case toImport
        case alreadyInTarget
    }

    private enum Row {
        case month(LegacyMonthPlan)
        case bundle(LegacyAssetBundle)
        case skippedHeader
        case candidate(LegacyFileCandidate)
    }

    private struct ActionStyle {
        let label: String
        let color: NSColor
        let reason: String?
        let reasonColor: NSColor
    }

    private let report: LegacyScanReport
    private let segmentedControl: NSSegmentedControl
    private let tableView = NSTableView()
    private let scrollView = NSScrollView()
    private let emptyLabel = NSTextField(labelWithString: "")
    private var rows: [Row] = []

    init(report: LegacyScanReport) {
        self.report = report
        segmentedControl = NSSegmentedControl(
            labels: ["", "", ""],
            trackingMode: .selectOne,
            target: nil,
            action: nil
        )
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func loadView() {
        view = NSView()

        let summary = makeSummaryView()

        segmentedControl.target = self
        segmentedControl.action = #selector(changeSegment(_:))
        segmentedControl.selectedSegment = ImportSegment.all.rawValue
        updateSegmentLabels()

        let column = NSTableColumn(
            identifier: NSUserInterfaceItemIdentifier("legacyScanResult")
        )
        column.title = ""
        tableView.addTableColumn(column)
        tableView.headerView = nil
        tableView.intercellSpacing = NSSize(width: 0, height: 1)
        tableView.selectionHighlightStyle = .none
        tableView.dataSource = self
        tableView.delegate = self

        scrollView.documentView = tableView
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.borderType = .noBorder

        emptyLabel.stringValue = String(
            localized: "migration.scan.empty.title"
        )
        emptyLabel.font = .systemFont(ofSize: 15, weight: .medium)
        emptyLabel.textColor = .secondaryLabelColor
        emptyLabel.alignment = .center
        emptyLabel.translatesAutoresizingMaskIntoConstraints = false

        let listContainer = NSView()
        scrollView.translatesAutoresizingMaskIntoConstraints = false
        listContainer.addSubview(scrollView)
        listContainer.addSubview(emptyLabel)
        NSLayoutConstraint.activate([
            scrollView.topAnchor.constraint(
                equalTo: listContainer.topAnchor
            ),
            scrollView.leadingAnchor.constraint(
                equalTo: listContainer.leadingAnchor
            ),
            scrollView.trailingAnchor.constraint(
                equalTo: listContainer.trailingAnchor
            ),
            scrollView.bottomAnchor.constraint(
                equalTo: listContainer.bottomAnchor
            ),
            emptyLabel.centerXAnchor.constraint(
                equalTo: listContainer.centerXAnchor
            ),
            emptyLabel.centerYAnchor.constraint(
                equalTo: listContainer.centerYAnchor
            ),
        ])

        let content = NSStackView(
            views: [summary, segmentedControl, listContainer]
        )
        content.orientation = .vertical
        content.alignment = .width
        content.spacing = 8
        content.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(content)

        NSLayoutConstraint.activate([
            content.topAnchor.constraint(equalTo: view.topAnchor),
            content.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            content.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            content.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            listContainer.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 260
            ),
        ])

        rebuildRows()
    }

    private func makeSummaryView() -> NSView {
        let totalBundles = report.plans.reduce(0) {
            $0 + $1.totalAssetCount
        }
        let totalBytes = report.plans.reduce(Int64(0)) {
            $0 + $1.totalFileSize
        }

        let summaryRow = NSStackView(
            views: [
                makeSummaryItem(
                    label: String(
                        localized: "migration.summary.bundles"
                    ),
                    value: Self.formatCount(totalBundles)
                ),
                makeSummaryItem(
                    label: String(localized: "migration.summary.total"),
                    value: ByteCountFormatter.fileSizeString(totalBytes)
                ),
                NSView(),
            ]
        )
        summaryRow.orientation = .horizontal
        summaryRow.alignment = .top
        summaryRow.spacing = 24

        let summary = NSStackView(views: [summaryRow])
        summary.orientation = .vertical
        summary.alignment = .leading
        summary.spacing = 4
        for warning in report.warnings.prefix(5) {
            let label = NSTextField(
                wrappingLabelWithString: warning
            )
            label.font = .systemFont(ofSize: 11)
            label.textColor = .wmMaterialWarningDetail
            label.maximumNumberOfLines = 2
            label.lineBreakMode = .byTruncatingMiddle
            summary.addArrangedSubview(label)
        }
        if report.warnings.count > 5 {
            let label = NSTextField(
                labelWithString: String(
                    format: String(
                        localized:
                            "migration.scan.moreWarnings.format"
                    ),
                    report.warnings.count - 5
                )
            )
            label.font = .systemFont(ofSize: 11)
            label.textColor = .secondaryLabelColor
            summary.addArrangedSubview(label)
        }
        return summary
    }

    private func makeSummaryItem(
        label: String,
        value: String
    ) -> NSView {
        let labelField = NSTextField(labelWithString: label)
        labelField.font = .systemFont(ofSize: 11)
        labelField.textColor = .secondaryLabelColor

        let valueField = NSTextField(labelWithString: value)
        valueField.font = .monospacedDigitSystemFont(
            ofSize: 13,
            weight: .regular
        )

        let stack = NSStackView(views: [labelField, valueField])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 0
        return stack
    }

    private func updateSegmentLabels() {
        for segment in [
            ImportSegment.all,
            .toImport,
            .alreadyInTarget,
        ] {
            segmentedControl.setLabel(
                segmentLabel(
                    segment,
                    count: count(for: segment)
                ),
                forSegment: segment.rawValue
            )
        }
    }

    private func rebuildRows() {
        let segment = ImportSegment(
            rawValue: segmentedControl.selectedSegment
        ) ?? .all
        rows = report.plans.flatMap { plan -> [Row] in
            let bundles = plan.bundles.filter {
                matches($0.action, segment: segment)
            }
            guard !bundles.isEmpty else { return [] }
            let visiblePlan = LegacyMonthPlan(
                id: plan.id,
                month: plan.month,
                bundles: bundles
            )
            return [.month(visiblePlan)]
                + bundles.map(Row.bundle)
        }
        if !report.unscheduledCandidates.isEmpty,
           segment != .toImport {
            rows.append(.skippedHeader)
            rows.append(
                contentsOf: report.unscheduledCandidates.map(
                    Row.candidate
                )
            )
        }
        tableView.reloadData()
        let isEmpty = rows.isEmpty
        scrollView.isHidden = isEmpty
        emptyLabel.isHidden = !isEmpty
    }

    private func count(for segment: ImportSegment) -> Int {
        report.plans.reduce(0) { count, plan in
            count + plan.bundles.lazy.filter {
                self.matches($0.action, segment: segment)
            }.count
        }
    }

    private func matches(
        _ action: LegacyBundleAction,
        segment: ImportSegment
    ) -> Bool {
        switch segment {
        case .all:
            return true
        case .toImport:
            switch action {
            case .insertNew, .replacesSubsets:
                return true
            case .skipExactMatch,
                 .skipEnclosed,
                 .skipPerceptualDuplicate:
                return false
            }
        case .alreadyInTarget:
            switch action {
            case .skipExactMatch,
                 .skipEnclosed,
                 .skipPerceptualDuplicate:
                return true
            case .insertNew, .replacesSubsets:
                return false
            }
        }
    }

    private func segmentLabel(
        _ segment: ImportSegment,
        count: Int
    ) -> String {
        let format: String
        switch segment {
        case .all:
            format = String(localized: "migration.scan.segment.all")
        case .toImport:
            format = String(
                localized: "migration.scan.segment.toImport"
            )
        case .alreadyInTarget:
            format = String(
                localized:
                    "migration.scan.segment.alreadyInTarget"
            )
        }
        return String(format: format, count)
    }

    private func actionStyle(
        for action: LegacyBundleAction
    ) -> ActionStyle {
        switch action {
        case .insertNew:
            return ActionStyle(
                label: String(localized: "migration.scan.action.new"),
                color: .wmMaterialPrimary,
                reason: nil,
                reasonColor: .secondaryLabelColor
            )
        case .skipExactMatch:
            return ActionStyle(
                label: String(localized: "migration.scan.action.skip"),
                color: .secondaryLabelColor,
                reason: String(
                    localized:
                        "migration.scan.reason.exactFingerprint"
                ),
                reasonColor: .secondaryLabelColor
            )
        case .skipEnclosed:
            return ActionStyle(
                label: String(localized: "migration.scan.action.skip"),
                color: .secondaryLabelColor,
                reason: String(
                    localized:
                        "migration.scan.reason.enclosedAsset"
                ),
                reasonColor: .secondaryLabelColor
            )
        case .skipPerceptualDuplicate:
            return ActionStyle(
                label: String(localized: "migration.scan.action.skip"),
                color: .secondaryLabelColor,
                reason: String(
                    localized:
                        "migration.scan.reason.perceptualDuplicate"
                ),
                reasonColor: .secondaryLabelColor
            )
        case .replacesSubsets(let count):
            return ActionStyle(
                label: String(
                    localized:
                        "migration.scan.action.replacesOlder"
                ),
                color: .wmMaterialWarningDetail,
                reason: String(
                    format: String(
                        localized:
                            "migration.scan.reason.subsetCount"
                    ),
                    count
                ),
                reasonColor: .wmMaterialWarningDetail
            )
        }
    }

    @objc private func changeSegment(_ sender: Any?) {
        rebuildRows()
    }

    private static let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm"
        return formatter
    }()

    private static let numberFormatter: NumberFormatter = {
        let formatter = NumberFormatter()
        formatter.numberStyle = .decimal
        return formatter
    }()

    private static func formatCount(_ value: Int) -> String {
        numberFormatter.string(from: NSNumber(value: value))
            ?? String(value)
    }
}

extension MacLegacyScanResultViewController:
    NSTableViewDataSource,
    NSTableViewDelegate
{
    func numberOfRows(in tableView: NSTableView) -> Int {
        rows.count
    }

    func tableView(
        _ tableView: NSTableView,
        heightOfRow row: Int
    ) -> CGFloat {
        guard rows.indices.contains(row) else { return 34 }
        switch rows[row] {
        case .month:
            return 34
        case .bundle:
            return 58
        case .skippedHeader:
            return 32
        case .candidate:
            return 34
        }
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        guard rows.indices.contains(row) else { return nil }
        switch rows[row] {
        case .month(let plan):
            return monthCell(plan)
        case .bundle(let bundle):
            return bundleCell(bundle)
        case .skippedHeader:
            return skippedHeaderCell()
        case .candidate(let candidate):
            return candidateCell(candidate)
        }
    }

    private func monthCell(_ plan: LegacyMonthPlan) -> NSView {
        let title = NSTextField(labelWithString: plan.month.text)
        title.font = .systemFont(ofSize: 13, weight: .semibold)
        title.textColor = NSColor.wmMaterialMonthTitle(
            for: plan.month.month
        )

        let summary = NSTextField(
            labelWithString: String(
                format: String(
                    localized: "migration.scan.monthSummary.format"
                ),
                plan.bundles.count,
                ByteCountFormatter.fileSizeString(
                    plan.totalFileSize
                )
            )
        )
        summary.font = .systemFont(ofSize: 11)
        summary.textColor = NSColor.wmMaterialMonthDetail(
            for: plan.month.month
        )

        let stack = NSStackView(views: [title, summary, NSView()])
        stack.orientation = .horizontal
        stack.alignment = .centerY
        stack.spacing = 6
        stack.translatesAutoresizingMaskIntoConstraints = false

        let cell = NSView()
        cell.wantsLayer = true
        cell.layer?.backgroundColor = NSColor.wmMaterialMonthSurface(
            for: plan.month.month
        ).cgColor
        cell.layer?.cornerRadius = 5
        cell.addSubview(stack)
        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(
                equalTo: cell.leadingAnchor,
                constant: 10
            ),
            stack.trailingAnchor.constraint(
                equalTo: cell.trailingAnchor,
                constant: -10
            ),
            stack.centerYAnchor.constraint(
                equalTo: cell.centerYAnchor
            ),
        ])
        return cell
    }

    private func bundleCell(_ bundle: LegacyAssetBundle) -> NSView {
        let style = actionStyle(for: bundle.action)
        let filename = NSTextField(
            labelWithString:
                bundle.resources.first?.originalFilename
                ?? "(unknown)"
        )
        filename.font = .monospacedSystemFont(
            ofSize: 12,
            weight: .regular
        )
        filename.lineBreakMode = .byTruncatingMiddle

        let detailParts = [
            bundle.creationDate.map {
                Self.dateFormatter.string(from: $0)
            },
            ByteCountFormatter.fileSizeString(
                bundle.totalFileSize
            ),
        ].compactMap { $0 }
        let detail = NSTextField(
            labelWithString: detailParts.joined(separator: "  ")
        )
        detail.font = .systemFont(ofSize: 11)
        detail.textColor = .secondaryLabelColor

        let labels = NSStackView(views: [filename, detail])
        if let reason = style.reason {
            let reasonLabel = NSTextField(
                labelWithString: reason
            )
            reasonLabel.font = .systemFont(ofSize: 10)
            reasonLabel.textColor = style.reasonColor
            labels.addArrangedSubview(reasonLabel)
        }
        labels.orientation = .vertical
        labels.alignment = .leading
        labels.spacing = 1
        labels.setContentCompressionResistancePriority(
            .defaultLow,
            for: .horizontal
        )

        let chipLabel = NSTextField(labelWithString: style.label)
        chipLabel.font = .systemFont(ofSize: 10, weight: .semibold)
        chipLabel.textColor = .white
        chipLabel.alignment = .center
        chipLabel.translatesAutoresizingMaskIntoConstraints = false

        let chip = NSBox()
        chip.boxType = .custom
        chip.borderWidth = 0
        chip.fillColor = style.color
        chip.cornerRadius = 8
        chip.contentViewMargins = NSSize(width: 7, height: 2)
        chip.contentView = chipLabel

        let row = NSStackView(views: [labels, NSView(), chip])
        row.orientation = .horizontal
        row.alignment = .centerY
        row.spacing = 8
        row.translatesAutoresizingMaskIntoConstraints = false

        let cell = NSView()
        cell.addSubview(row)
        NSLayoutConstraint.activate([
            row.leadingAnchor.constraint(
                equalTo: cell.leadingAnchor,
                constant: 10
            ),
            row.trailingAnchor.constraint(
                equalTo: cell.trailingAnchor,
                constant: -10
            ),
            row.centerYAnchor.constraint(
                equalTo: cell.centerYAnchor
            ),
        ])
        return cell
    }

    private func skippedHeaderCell() -> NSView {
        let label = NSTextField(
            labelWithString: String(
                localized: "migration.skipped.section"
            )
        )
        label.font = .systemFont(ofSize: 12, weight: .semibold)
        label.textColor = .secondaryLabelColor
        label.translatesAutoresizingMaskIntoConstraints = false

        let cell = NSView()
        cell.addSubview(label)
        NSLayoutConstraint.activate([
            label.leadingAnchor.constraint(
                equalTo: cell.leadingAnchor,
                constant: 10
            ),
            label.centerYAnchor.constraint(
                equalTo: cell.centerYAnchor
            ),
        ])
        return cell
    }

    private func candidateCell(
        _ candidate: LegacyFileCandidate
    ) -> NSView {
        let icon = NSImageView()
        icon.image = NSImage(
            systemSymbolName: "exclamationmark.triangle.fill",
            accessibilityDescription: nil
        )
        icon.contentTintColor = .wmMaterialWarningDetail

        let filename = NSTextField(
            labelWithString: candidate.originalFilename
        )
        filename.lineBreakMode = .byTruncatingMiddle
        filename.setContentCompressionResistancePriority(
            .defaultLow,
            for: .horizontal
        )

        let size = NSTextField(
            labelWithString: ByteCountFormatter.fileSizeString(
                candidate.fileSize
            )
        )
        size.font = .systemFont(ofSize: 11)
        size.textColor = .secondaryLabelColor

        let row = NSStackView(
            views: [icon, filename, NSView(), size]
        )
        row.orientation = .horizontal
        row.alignment = .centerY
        row.spacing = 8
        row.translatesAutoresizingMaskIntoConstraints = false

        let cell = NSView()
        cell.addSubview(row)
        NSLayoutConstraint.activate([
            row.leadingAnchor.constraint(
                equalTo: cell.leadingAnchor,
                constant: 10
            ),
            row.trailingAnchor.constraint(
                equalTo: cell.trailingAnchor,
                constant: -10
            ),
            row.centerYAnchor.constraint(
                equalTo: cell.centerYAnchor
            ),
        ])
        return cell
    }
}
