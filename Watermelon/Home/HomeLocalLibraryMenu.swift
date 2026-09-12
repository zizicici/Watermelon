import UIKit

@MainActor
enum HomeLocalLibraryMenu {
    static func deviceTitle(for filter: PhotoLibraryMediaFilter, isPad: Bool) -> String {
        let device = isPad ? "iPad" : "iPhone"
        switch filter {
        case .videos:
            return String.localizedStringWithFormat(String(localized: "home.localSource.deviceVideos"), device)
        case .photos:
            return String.localizedStringWithFormat(String(localized: "home.localSource.devicePhotos"), device)
        case .all:
            return device
        }
    }

    static func deviceMenu(
        scope: HomeLocalLibraryScope,
        isPad: Bool,
        attributes: UIMenuElement.Attributes,
        onSelect: @escaping (HomeLocalLibraryScope) -> Void
    ) -> UIMenu {
        let actions = PhotoLibraryMediaFilter.allCases.map { filter in
            UIAction(
                title: filter.localizedTitle,
                image: UIImage(systemName: symbolName(for: filter)),
                attributes: attributes,
                state: scope.deviceMediaFilter == filter ? .on : .off
            ) { _ in
                onSelect(.device(filter))
            }
        }
        return UIMenu(
            title: isPad ? "iPad" : "iPhone",
            subtitle: scope.deviceMediaFilter?.localizedTitle,
            image: UIImage(systemName: isPad ? "ipad" : "iphone"),
            children: actions
        )
    }

    private static func symbolName(for filter: PhotoLibraryMediaFilter) -> String {
        switch filter {
        case .videos: return "video"
        case .photos: return "photo"
        case .all: return "photo.on.rectangle"
        }
    }
}
