import Foundation
import MoreKit
import UIKit

enum MonthGroupingTimeZoneFormatter {
    static func title(for preference: MonthGroupingTimeZonePreference) -> String {
        switch preference.mode {
        case .system:
            return String(localized: "settings.monthGroupingTimeZone.system", defaultValue: "Follow Local System Time Zone")
        case .fixedOffset where preference.offsetSeconds == 0:
            return "UTC"
        case .fixedIana, .fixedOffset:
            return summary(for: preference.effectiveTimeZone)
        }
    }

    static func summary(for timeZone: TimeZone, date: Date = Date()) -> String {
        if isUTCDisplayTimeZone(timeZone), timeZone.secondsFromGMT(for: date) == 0 {
            return "UTC"
        }
        let name = displayName(for: timeZone)
        let offset = gmtOffsetText(for: timeZone, date: date)
        if name == offset {
            return offset
        }
        return "\(name) (\(offset))"
    }

    static func detail(for timeZone: TimeZone, date: Date = Date()) -> String {
        gmtOffsetText(for: timeZone, date: date)
    }

    static func displayName(for timeZone: TimeZone) -> String {
        if isUTCDisplayTimeZone(timeZone) {
            return "UTC"
        }
        return timeZone.identifier
    }

    static func cityName(from identifier: String) -> String {
        let raw = identifier.split(separator: "/").last.map(String.init) ?? identifier
        return raw.replacingOccurrences(of: "_", with: " ")
    }

    static func gmtOffsetText(for timeZone: TimeZone, date: Date = Date()) -> String {
        let seconds = timeZone.secondsFromGMT(for: date)
        let sign = seconds >= 0 ? "+" : "-"
        let absolute = abs(seconds)
        let hours = absolute / 3600
        let minutes = (absolute % 3600) / 60
        return String(format: "GMT%@%02d:%02d", sign, hours, minutes)
    }

    private static func isUTCDisplayTimeZone(_ timeZone: TimeZone) -> Bool {
        timeZone.identifier == "GMT"
    }
}

final class MonthGroupingTimeZoneSettingsViewController: UITableViewController {
    private enum Section: Int, CaseIterable {
        case current
        case options
    }

    private enum OptionRow: Int, CaseIterable {
        case system
        case custom
    }

    private let cellIdentifier = "MonthGroupingTimeZoneCell"
    private let canChangePreference: () -> Bool

    init(canChangePreference: @escaping () -> Bool = { true }) {
        self.canChangePreference = canChangePreference
        super.init(style: .insetGrouped)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "settings.monthGroupingTimeZone.title", defaultValue: "Local Photo Grouping Time Zone")
        view.backgroundColor = .appBackground
        tableView.backgroundColor = .appBackground
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: cellIdentifier)
    }

    override func numberOfSections(in tableView: UITableView) -> Int {
        Section.allCases.count
    }

    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        switch Section(rawValue: section) ?? .current {
        case .current:
            return 1
        case .options:
            return OptionRow.allCases.count
        }
    }

    override func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard Section(rawValue: section) == .options else { return nil }
        return String(
            localized: "settings.monthGroupingTimeZone.footer",
            defaultValue: "Decides which month local photos belong to on this device. Changing it affects local grouping, selection scopes, and future backup grouping on this device; existing remote month files are not moved."
        )
    }

    override func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: cellIdentifier, for: indexPath)
        let preference = MonthGroupingTimeZonePreference.current

        var content = UIListContentConfiguration.valueCell()
        content.textProperties.color = .label
        content.secondaryTextProperties.color = .secondaryLabel
        cell.tintColor = .appTint
        cell.selectionStyle = .default
        cell.accessoryType = .none

        switch Section(rawValue: indexPath.section) ?? .current {
        case .current:
            content.text = String(localized: "settings.monthGroupingTimeZone.section.timeZone", defaultValue: "Time Zone")
            content.secondaryText = MonthGroupingTimeZoneFormatter.title(for: preference)
            cell.selectionStyle = .none
        case .options:
            let row = OptionRow(rawValue: indexPath.row) ?? .system
            configureOptionCell(row, preference: preference, content: &content, cell: cell)
        }

        cell.contentConfiguration = content
        return cell
    }

    private func configureOptionCell(
        _ row: OptionRow,
        preference: MonthGroupingTimeZonePreference,
        content: inout UIListContentConfiguration,
        cell: UITableViewCell
    ) {
        switch row {
        case .system:
            content.text = String(localized: "settings.monthGroupingTimeZone.system", defaultValue: "Follow Local System Time Zone")
            content.secondaryText = nil
            cell.accessoryType = preference.mode == .system ? .checkmark : .none
        case .custom:
            content.text = String(localized: "settings.monthGroupingTimeZone.chooseOther", defaultValue: "Custom Time Zone")
            content.secondaryText = preference.mode == .system ? nil : MonthGroupingTimeZoneFormatter.title(for: preference)
            cell.accessoryType = .disclosureIndicator
        }
    }

    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard Section(rawValue: indexPath.section) == .options,
              let row = OptionRow(rawValue: indexPath.row) else { return }

        switch row {
        case .system:
            setPreference(.defaultPreference)
        case .custom:
            guard canChangePreference() else {
                showChangeBlockedAlert()
                return
            }
            let picker = MonthGroupingTimeZonePickerViewController { [weak self] preference in
                if self?.setPreference(preference) == true {
                    self?.navigationController?.popViewController(animated: true)
                }
            }
            navigationController?.pushViewController(picker, animated: true)
        }
    }

    @discardableResult
    private func setPreference(_ preference: MonthGroupingTimeZonePreference) -> Bool {
        let normalized = preference.normalized()
        guard normalized != MonthGroupingTimeZonePreference.current else {
            tableView.reloadData()
            return true
        }
        guard canChangePreference() else {
            showChangeBlockedAlert()
            return false
        }

        MonthGroupingTimeZonePreference.setCurrent(normalized)
        tableView.reloadData()
        return true
    }

    private func showChangeBlockedAlert() {
        let alert = UIAlertController(
            title: String(localized: "settings.monthGroupingTimeZone.title", defaultValue: "Local Photo Grouping Time Zone"),
            message: String(localized: "settings.monthGroupingTimeZone.blockedDuringExecution", defaultValue: "Stop the current operation before changing this setting."),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok", defaultValue: "OK"), style: .default))
        (navigationController?.topViewController ?? self).present(alert, animated: true)
    }
}
