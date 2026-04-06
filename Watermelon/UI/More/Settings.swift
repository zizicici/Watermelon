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
        case BackupWorkerCountMode = "com.zizicici.common.settings.BackupWorkerCountMode"
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

enum BackupWorkerCountMode: Int, CaseIterable, Codable {
    case automatic = 0
    case one
    case two
    case three
    case four

    var workerCountOverride: Int? {
        switch self {
        case .automatic:
            return nil
        case .one:
            return 1
        case .two:
            return 2
        case .three:
            return 3
        case .four:
            return 4
        }
    }
}

extension BackupWorkerCountMode: UserDefaultSettable {
    static func getKey() -> UserDefaults.Settings {
        .BackupWorkerCountMode
    }

    static var defaultOption: BackupWorkerCountMode {
        .automatic
    }

    static func getHeader() -> String? {
        "上传并发 Worker"
    }

    static func getFooter() -> String? {
        "自动模式会按存储协议使用默认并发（SMB/WebDAV=2，本地存储=3）。手动模式会覆盖协议默认值。"
    }

    func getName() -> String {
        switch self {
        case .automatic:
            return "自动（按协议）"
        case .one:
            return "1 个 Worker"
        case .two:
            return "2 个 Worker"
        case .three:
            return "3 个 Worker"
        case .four:
            return "4 个 Worker"
        }
    }

    static func getTitle() -> String {
        "上传并发"
    }
}
