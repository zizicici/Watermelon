import UIKit

final class MonthGroupingTimeZonePickerViewController: UITableViewController, UISearchResultsUpdating {
    private enum Section: Int, CaseIterable {
        case quick
        case timeZones
    }

    private struct Choice: Hashable, Sendable {
        enum Kind: Hashable, Sendable {
            case fixedCurrent
            case utc
            case timeZone(String)
        }

        let kind: Kind
        let title: String
        let detail: String
        let searchText: String

        var identifier: String? {
            if case .timeZone(let identifier) = kind {
                return identifier
            }
            return nil
        }
    }

    private struct ChoiceGroups: Sendable {
        let quick: [Choice]
        let timeZones: [Choice]
    }

    private let cellIdentifier = "TimeZoneChoiceCell"
    private let onSelect: (MonthGroupingTimeZonePreference) -> Void
    private let loadingIndicator = UIActivityIndicatorView(style: .medium)
    private var choicesTask: Task<Void, Never>?
    private var searchController: UISearchController?
    private var allQuickChoices: [Choice] = []
    private var allTimeZoneChoices: [Choice] = []
    private var filteredQuickChoices: [Choice] = []
    private var filteredTimeZoneChoices: [Choice] = []

    init(onSelect: @escaping (MonthGroupingTimeZonePreference) -> Void) {
        self.onSelect = onSelect
        super.init(style: .insetGrouped)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        choicesTask?.cancel()
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "settings.monthGroupingTimeZone.chooseTitle", defaultValue: "Choose Time Zone")
        view.backgroundColor = .appBackground
        tableView.backgroundColor = .appBackground
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: cellIdentifier)

        let searchController = UISearchController(searchResultsController: nil)
        searchController.obscuresBackgroundDuringPresentation = false
        searchController.searchResultsUpdater = self
        searchController.searchBar.placeholder = String(localized: "settings.monthGroupingTimeZone.search", defaultValue: "Search city or time zone")
        navigationItem.searchController = searchController
        self.searchController = searchController
        navigationItem.hidesSearchBarWhenScrolling = false
        definesPresentationContext = true

        showLoadingChoices()
        loadChoices()
    }

    override func numberOfSections(in tableView: UITableView) -> Int {
        Section.allCases.count
    }

    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        switch Section(rawValue: section) ?? .quick {
        case .quick:
            return filteredQuickChoices.count
        case .timeZones:
            return filteredTimeZoneChoices.count
        }
    }

    override func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: cellIdentifier, for: indexPath)
        let choice = choice(at: indexPath)
        var content = cell.defaultContentConfiguration()
        content.text = choice.title
        content.secondaryText = choice.detail
        cell.contentConfiguration = content
        cell.tintColor = .appTint
        cell.accessoryType = Self.isSelected(choice, current: MonthGroupingTimeZonePreference.current) ? .checkmark : .none
        return cell
    }

    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        let choice = choice(at: indexPath)
        switch choice.kind {
        case .fixedCurrent:
            onSelect(.fixedCurrent())
        case .utc:
            onSelect(.fixedUTC())
        case .timeZone(let identifier):
            guard let timeZone = TimeZone(identifier: identifier) else { return }
            onSelect(MonthGroupingTimeZonePreference(
                mode: .fixedIana,
                identifier: identifier,
                fallbackOffsetSeconds: timeZone.secondsFromGMT(for: Date())
            ))
        }
    }

    func updateSearchResults(for searchController: UISearchController) {
        applySearchQuery(searchController.searchBar.text ?? "")
        tableView.reloadData()
    }

    private func choice(at indexPath: IndexPath) -> Choice {
        switch Section(rawValue: indexPath.section) ?? .quick {
        case .quick:
            return filteredQuickChoices[indexPath.row]
        case .timeZones:
            return filteredTimeZoneChoices[indexPath.row]
        }
    }

    private func loadChoices() {
        choicesTask?.cancel()
        choicesTask = Task { [weak self] in
            let groups = await Task.detached(priority: .userInitiated) {
                Self.makeChoiceGroups()
            }.value
            guard !Task.isCancelled else { return }
            await MainActor.run {
                guard let self else { return }
                self.allQuickChoices = groups.quick
                self.allTimeZoneChoices = groups.timeZones
                self.applySearchQuery(self.searchController?.searchBar.text ?? "")
                self.hideLoadingChoices()
                self.tableView.reloadData()
            }
        }
    }

    private func applySearchQuery(_ rawQuery: String) {
        let query = Self.normalizedSearchText([rawQuery.trimmingCharacters(in: .whitespacesAndNewlines)])
        guard !query.isEmpty else {
            filteredQuickChoices = allQuickChoices
            filteredTimeZoneChoices = allTimeZoneChoices
            return
        }

        filteredQuickChoices = allQuickChoices.filter { $0.searchText.contains(query) }
        filteredTimeZoneChoices = allTimeZoneChoices.filter { $0.searchText.contains(query) }
    }

    private func showLoadingChoices() {
        loadingIndicator.startAnimating()
        tableView.backgroundView = loadingIndicator
    }

    private func hideLoadingChoices() {
        loadingIndicator.stopAnimating()
        tableView.backgroundView = nil
    }

    private static func isSelected(_ choice: Choice, current: MonthGroupingTimeZonePreference) -> Bool {
        switch choice.kind {
        case .fixedCurrent:
            return current.mode == .fixedIana && current.identifier == TimeZone.current.identifier
        case .utc:
            return current.mode == .fixedOffset && current.offsetSeconds == 0
        case .timeZone(let identifier):
            if current.mode == .fixedIana,
               current.identifier == TimeZone.current.identifier,
               identifier == current.identifier {
                return false
            }
            return identifier == current.identifier
        }
    }

    nonisolated private static func makeChoiceGroups() -> ChoiceGroups {
        let now = Date()
        let current = MonthGroupingTimeZonePreference.currentSystemTimeZone()
        let quickChoices = makeQuickChoices(current: current, now: now)
        let timeZoneChoices = MonthGroupingTimeZoneCatalog.selectableIdentifiers(
            adding: [
                current.identifier,
                MonthGroupingTimeZonePreference.current.identifier
            ]
        )
        .compactMap { identifier -> Choice? in
            guard identifier != "GMT",
                  let timeZone = TimeZone(identifier: identifier) else { return nil }
            let title = MonthGroupingTimeZoneFormatter.displayName(for: timeZone)
            let city = MonthGroupingTimeZoneFormatter.cityName(from: identifier)
            let detail = MonthGroupingTimeZoneFormatter.detail(for: timeZone, date: now)
            let searchText = searchText(for: timeZone, identifier: identifier, title: title, city: city, detail: detail, date: now)
            return Choice(kind: .timeZone(identifier), title: title, detail: detail, searchText: searchText)
        }
        .sorted {
            let lhsOffset = offsetSeconds(for: $0, date: now)
            let rhsOffset = offsetSeconds(for: $1, date: now)
            if lhsOffset != rhsOffset {
                return lhsOffset < rhsOffset
            }
            return ($0.identifier ?? "").localizedCaseInsensitiveCompare($1.identifier ?? "") == .orderedAscending
        }
        return ChoiceGroups(quick: quickChoices, timeZones: timeZoneChoices)
    }

    nonisolated private static func makeQuickChoices(current: TimeZone, now: Date) -> [Choice] {
        let currentTitle = String(localized: "settings.monthGroupingTimeZone.fixedCurrent", defaultValue: "Fix to Current Time Zone")
        let currentDetail = MonthGroupingTimeZoneFormatter.summary(for: current, date: now)
        let currentSearchText = searchText(
            for: current,
            identifier: current.identifier,
            title: currentTitle,
            city: MonthGroupingTimeZoneFormatter.cityName(from: current.identifier),
            detail: currentDetail,
            date: now
        )
        let utcDetail = "GMT+00:00"
        let utcSearchText = normalizedSearchText(["UTC", utcDetail, "GMT"])
        return [
            Choice(kind: .fixedCurrent, title: currentTitle, detail: currentDetail, searchText: currentSearchText),
            Choice(kind: .utc, title: "UTC", detail: utcDetail, searchText: utcSearchText)
        ]
    }

    nonisolated private static func searchText(
        for timeZone: TimeZone,
        identifier: String,
        title: String,
        city: String,
        detail: String,
        date: Date
    ) -> String {
        normalizedSearchText([
            identifier,
            title,
            detail,
            city,
            MonthGroupingTimeZoneFormatter.gmtOffsetText(for: timeZone, date: date),
            timeZone.abbreviation(for: date) ?? ""
        ])
    }

    nonisolated private static func normalizedSearchText(_ parts: [String]) -> String {
        parts
            .joined(separator: " ")
            .folding(options: [.caseInsensitive, .diacriticInsensitive, .widthInsensitive], locale: .current)
    }

    nonisolated private static func offsetSeconds(for choice: Choice, date: Date) -> Int {
        guard let identifier = choice.identifier,
              let timeZone = TimeZone(identifier: identifier) else {
            return 0
        }
        return timeZone.secondsFromGMT(for: date)
    }

}
