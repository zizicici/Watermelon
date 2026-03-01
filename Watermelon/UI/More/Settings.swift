//
//  Settings.swift
//  Watermelon
//
//  Created by zici on 2/5/24.
//

import Foundation

extension UserDefaults {
    enum Settings: String {
        case TutorialEntranceType = "com.zizicici.common.settings.TutorialEntranceType"
    }
}

extension Notification.Name {
    static let SettingsUpdate = Notification.Name(rawValue: "com.zizicici.common.settings.updated")
}

protocol SettingsOption: Hashable, Equatable {
    func getName() -> String
    static func getHeader() -> String?
    static func getFooter() -> String?
    static func getTitle() -> String
    static func getOptions() -> [Self]
    static var current: Self { get set}
}

extension SettingsOption {
    static func getHeader() -> String? {
        return nil
    }
    
    static func getFooter() -> String? {
        return nil
    }
}

extension SettingsOption {
    static func == (lhs: Self, rhs: Self) -> Bool {
        if type(of: lhs) != type(of: rhs) {
            return false
        } else {
            return lhs.hashValue == rhs.hashValue
        }
    }
}

protocol UserDefaultSettable: SettingsOption {
    static func getKey() -> UserDefaults.Settings
    static var defaultOption: Self { get }
}

extension UserDefaultSettable where Self: RawRepresentable, Self.RawValue == Int {
    static func getValue() -> Self {
        if let intValue = UserDefaults.standard.getInt(forKey: getKey().rawValue), let value = Self(rawValue: intValue) {
            return value
        } else {
            return defaultOption
        }
    }
    
    static func setValue(_ value: Self) {
        UserDefaults.standard.set(value.rawValue, forKey: getKey().rawValue)
        NotificationCenter.default.post(name: Notification.Name.SettingsUpdate, object: nil)
    }
    
    static func getOptions<T: CaseIterable>() -> [T] {
        return Array(T.allCases)
    }
    
    static var current: Self {
        get {
            return getValue()
        }
        set {
            setValue(newValue)
        }
    }
}

extension UserDefaults {
    func getInt(forKey key: String) -> Int? {
        return object(forKey: key) as? Int
    }
    
    func getBool(forKey key: String) -> Bool? {
        return object(forKey: key) as? Bool
    }
    
    func getString(forKey key: String) -> String? {
        return object(forKey: key) as? String
    }
}

enum TutorialEntranceType: Int, CaseIterable, Codable {
    case firstTab = 0
    case secondTab
    case hidden
}

extension TutorialEntranceType: UserDefaultSettable {
    static func getKey() -> UserDefaults.Settings {
        .TutorialEntranceType
    }
    
    static var defaultOption: TutorialEntranceType {
        return .firstTab
    }
    
    func getName() -> String {
        switch self {
        case .firstTab:
            return String(localized: "settings.tutorialEntranceType.first")
        case .secondTab:
            return String(localized: "settings.tutorialEntranceType.second")
        case .hidden:
            return String(localized: "settings.tutorialEntranceType.hidden")
        }
    }
    
    static func getTitle() -> String {
        return String(localized: "settings.tutorialEntranceType.title")
    }
}
