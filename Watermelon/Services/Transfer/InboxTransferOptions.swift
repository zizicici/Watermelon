import Foundation
import MoreKit

struct InboxTransferAccessPolicy: Equatable, Sendable {
    static let freeMaximumItemCount = 3

    let isPro: Bool

    func allows(itemCount: Int) -> Bool {
        isPro || itemCount <= Self.freeMaximumItemCount
    }

    var buttonSubtitle: String? {
        isPro ? nil : String(localized: "transfer.limit.free")
    }
}

struct InboxTransferOptions: RawRepresentable, Hashable, Sendable {
    enum Option: Hashable, Sendable {
        case includeLivePhotoVideo
        case useOriginalEditedPhoto
        case useOriginalEditedVideo
        case removeLocationMetadata
    }

    private static let livePhotoVideoBit = 1 << 0
    private static let originalEditedPhotoBit = 1 << 1
    private static let originalEditedVideoBit = 1 << 2
    private static let removeLocationMetadataBit = 1 << 3
    private static let allMask = (1 << 4) - 1

    let rawValue: Int

    init(rawValue: Int) {
        self.rawValue = rawValue & Self.allMask
    }

    var includesLivePhotoVideo: Bool {
        contains(Self.livePhotoVideoBit)
    }

    var usesOriginalEditedPhoto: Bool {
        contains(Self.originalEditedPhotoBit)
    }

    var usesOriginalEditedVideo: Bool {
        contains(Self.originalEditedVideoBit)
    }

    var removesLocationMetadata: Bool {
        contains(Self.removeLocationMetadataBit)
    }

    func contains(_ option: Option) -> Bool {
        contains(bit(for: option))
    }

    func updating(_ option: Option, isEnabled: Bool) -> InboxTransferOptions {
        let optionBit = bit(for: option)
        return InboxTransferOptions(rawValue: isEnabled ? rawValue | optionBit : rawValue & ~optionBit)
    }

    private func contains(_ bit: Int) -> Bool {
        rawValue & bit != 0
    }

    private func bit(for option: Option) -> Int {
        switch option {
        case .includeLivePhotoVideo:
            return Self.livePhotoVideoBit
        case .useOriginalEditedPhoto:
            return Self.originalEditedPhotoBit
        case .useOriginalEditedVideo:
            return Self.originalEditedVideoBit
        case .removeLocationMetadata:
            return Self.removeLocationMetadataBit
        }
    }
}

protocol InboxTransferBinarySetting: SettingsOption, CaseIterable, RawRepresentable
where RawValue == Int {
    static var option: InboxTransferOptions.Option { get }
    static var settingTitle: String { get }
    static var settingFooter: String { get }
}

extension InboxTransferBinarySetting {
    func getName() -> String {
        if rawValue == 1 {
            return String(localized: "settings.common.enable")
        }
        return String(localized: "transfer.settings.option.off")
    }

    static func getTitle() -> String {
        settingTitle
    }

    static func getHeader() -> String? {
        getTitle()
    }

    static func getFooter() -> String? {
        settingFooter
    }

    static func getOptions() -> [Self] {
        [Self(rawValue: 1), Self(rawValue: 0)].compactMap { $0 }
    }

    static var current: Self {
        Self(rawValue: InboxTransferOptions.storedValue.contains(option) ? 1 : 0)!
    }

    static func setCurrent(_ value: Self) throws {
        InboxTransferOptions.store(
            InboxTransferOptions.storedValue.updating(option, isEnabled: value.rawValue == 1)
        )
    }
}

enum InboxTransferLivePhotoVideoSetting: Int, CaseIterable, InboxTransferBinarySetting {
    case disable = 0
    case enable

    static let option = InboxTransferOptions.Option.includeLivePhotoVideo
    static var settingTitle: String { String(localized: "transfer.filter.includeLiveVideo") }
    static var settingFooter: String { String(localized: "transfer.settings.livePhoto.footer") }
}

enum InboxTransferOriginalPhotoSetting: Int, CaseIterable, InboxTransferBinarySetting {
    case disable = 0
    case enable

    static let option = InboxTransferOptions.Option.useOriginalEditedPhoto
    static var settingTitle: String { String(localized: "transfer.filter.originalPhoto") }
    static var settingFooter: String { String(localized: "transfer.settings.originalPhoto.footer") }
}

enum InboxTransferOriginalVideoSetting: Int, CaseIterable, InboxTransferBinarySetting {
    case disable = 0
    case enable

    static let option = InboxTransferOptions.Option.useOriginalEditedVideo
    static var settingTitle: String { String(localized: "transfer.filter.originalVideo") }
    static var settingFooter: String { String(localized: "transfer.settings.originalVideo.footer") }
}

enum InboxTransferRemoveLocationSetting: Int, CaseIterable, InboxTransferBinarySetting {
    case disable = 0
    case enable

    static let option = InboxTransferOptions.Option.removeLocationMetadata
    static var settingTitle: String { String(localized: "transfer.filter.removeLocation") }
    static var settingFooter: String { String(localized: "transfer.settings.privacy.footer") }
}

extension InboxTransferOptions: UserDefaultSettable {
    static func getKey() -> String {
        "com.zizicici.common.settings.InboxTransferOptions"
    }

    static var defaultOption: InboxTransferOptions {
        InboxTransferOptions(rawValue:
            livePhotoVideoBit |
            originalEditedPhotoBit |
            originalEditedVideoBit
        )
    }

    static func getHeader() -> String? {
        String(localized: "transfer.settings.title")
    }

    static func getFooter() -> String? {
        String(localized: "transfer.settings.footer")
    }

    func getName() -> String {
        let livePhoto = includesLivePhotoVideo
            ? String(localized: "transfer.summary.liveVideo")
            : String(localized: "transfer.summary.stillOnly")
        let editedMedia: String
        if usesOriginalEditedPhoto && usesOriginalEditedVideo {
            editedMedia = String(localized: "transfer.summary.originals")
        } else if !usesOriginalEditedPhoto && !usesOriginalEditedVideo {
            editedMedia = String(localized: "transfer.summary.editedVersions")
        } else {
            editedMedia = String(localized: "transfer.summary.mixedVersions")
        }
        let privacy = removesLocationMetadata
            ? String(localized: "transfer.summary.locationRemoved")
            : String(localized: "transfer.summary.locationKept")
        return ListFormatter.localizedString(byJoining: [livePhoto, editedMedia, privacy])
    }

    static func getTitle() -> String {
        String(localized: "transfer.settings.title")
    }

    static func getOptions() -> [InboxTransferOptions] {
        [defaultOption]
    }

    static var storedValue: InboxTransferOptions {
        getValue()
    }

    static func store(_ value: InboxTransferOptions) {
        setValue(value)
    }
}
