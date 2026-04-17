//
//  Settings.swift
//  Watermelon
//
//  Created by zici on 2/5/24.
//

import Foundation
import MoreKit

// MARK: - App Name

enum AppName {
    static var localized: String {
        if let name = Bundle.main.localizedInfoDictionary?["CFBundleDisplayName"] as? String, !name.isEmpty {
            return name
        }
        if let name = Bundle.main.infoDictionary?["CFBundleDisplayName"] as? String, !name.isEmpty {
            return name
        }
        return "Watermelon"
    }
}

// MARK: - BackupWorkerCountMode

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
    static func getKey() -> String {
        "com.zizicici.common.settings.BackupWorkerCountMode"
    }

    static var defaultOption: BackupWorkerCountMode {
        .automatic
    }

    static func getHeader() -> String? {
        String(localized: "settings.worker.header")
    }

    static func getFooter() -> String? {
        String(localized: "settings.worker.footer")
    }

    func getName() -> String {
        switch self {
        case .automatic:
            return String(localized: "settings.worker.automatic")
        case .one:
            return String(format: String(localized: "settings.worker.count"), 1)
        case .two:
            return String(format: String(localized: "settings.worker.count"), 2)
        case .three:
            return String(format: String(localized: "settings.worker.count"), 3)
        case .four:
            return String(format: String(localized: "settings.worker.count"), 4)
        }
    }

    static func getTitle() -> String {
        String(localized: "settings.worker.title")
    }
}

// MARK: - iCloud Photo Backup

enum ICloudPhotoBackupMode: Int, CaseIterable, Codable, Sendable {
    case disable = 0
    case enable

    var allowsNetworkAccess: Bool {
        self == .enable
    }
}

extension ICloudPhotoBackupMode: UserDefaultSettable {
    static func getKey() -> String {
        "com.zizicici.common.settings.ICloudPhotoBackupMode"
    }

    static var defaultOption: ICloudPhotoBackupMode {
        .disable
    }

    static func getHeader() -> String? {
        String(localized: "settings.icloud.header")
    }

    static func getFooter() -> String? {
        String(format: String(localized: "settings.icloud.footer"), AppName.localized)
    }

    func getName() -> String {
        switch self {
        case .disable:
            return String(localized: "settings.common.disable")
        case .enable:
            return String(localized: "settings.common.enable")
        }
    }

    static func getTitle() -> String {
        String(localized: "settings.icloud.header")
    }
}

// MARK: - Background Backup

enum BackgroundBackupSetting: Int, CaseIterable, Codable {
    case disable = 0
    case enable
}

extension BackgroundBackupSetting: UserDefaultSettable {
    static func getKey() -> String {
        "com.zizicici.common.settings.BackgroundBackupSetting"
    }

    static var defaultOption: BackgroundBackupSetting {
        .disable
    }

    static func getHeader() -> String? {
        String(localized: "settings.background.header")
    }

    static func getFooter() -> String? {
        String(format: String(localized: "settings.background.footer"), AppName.localized)
    }

    func getName() -> String {
        switch self {
        case .disable:
            return String(localized: "settings.common.disable")
        case .enable:
            return String(localized: "settings.common.enable")
        }
    }

    static func getTitle() -> String {
        String(localized: "settings.background.header")
    }

    @MainActor
    static func setCurrent(_ value: BackgroundBackupSetting) throws {
        if value == .enable && !ProStatus.isPro {
            throw BackgroundBackupSettingError.requiresPro
        }
        setValue(value)
    }
}

enum BackgroundBackupSettingError: LocalizedError {
    case requiresPro

    var errorDescription: String? {
        String(localized: "settings.background.requiresPro")
    }
}

enum PiPProgressSetting: Int, CaseIterable, Codable {
    case disable = 0
    case enable
}

extension PiPProgressSetting: UserDefaultSettable {
    static func getKey() -> String {
        "com.zizicici.common.settings.PiPProgressSetting"
    }

    static var defaultOption: PiPProgressSetting {
        .disable
    }

    static func getHeader() -> String? {
        String(localized: "settings.pipProgress.header")
    }

    static func getFooter() -> String? {
        String(localized: "settings.pipProgress.footer")
    }

    func getName() -> String {
        switch self {
        case .disable:
            return String(localized: "settings.common.disable")
        case .enable:
            return String(localized: "settings.common.enable")
        }
    }

    static func getTitle() -> String {
        String(localized: "settings.pipProgress.header")
    }

    @MainActor
    static func setCurrent(_ value: PiPProgressSetting) throws {
        if value == .enable && !ProStatus.isPro {
            throw PiPProgressSettingError.requiresPro
        }
        setValue(value)
    }
}

enum PiPProgressSettingError: LocalizedError {
    case requiresPro

    var errorDescription: String? {
        String(localized: "settings.pipProgress.requiresPro")
    }
}

// MARK: - Execution Log Filter

struct ExecutionLogFilterPreference: RawRepresentable, Hashable, Sendable {
    let rawValue: Int

    init(rawValue: Int) {
        self.rawValue = rawValue & Self.allMask
    }

    static let defaultOption = ExecutionLogFilterPreference(rawValue: allMask)

    private static let allMask = ExecutionLogLevel.allCases.reduce(0) { partial, level in
        partial | bit(for: level)
    }

    var enabledLevels: Set<ExecutionLogLevel> {
        Set(
            ExecutionLogLevel.allCases.filter { level in
                rawValue & Self.bit(for: level) != 0
            }
        )
    }

    func contains(_ level: ExecutionLogLevel) -> Bool {
        rawValue & Self.bit(for: level) != 0
    }

    func updating(_ level: ExecutionLogLevel, isEnabled: Bool) -> ExecutionLogFilterPreference {
        let bit = Self.bit(for: level)
        let updatedRawValue = isEnabled ? (rawValue | bit) : (rawValue & ~bit)
        return ExecutionLogFilterPreference(rawValue: updatedRawValue)
    }

    static func all() -> ExecutionLogFilterPreference {
        defaultOption
    }

    private static func bit(for level: ExecutionLogLevel) -> Int {
        switch level {
        case .debug:
            return 1 << 0
        case .info:
            return 1 << 1
        case .warning:
            return 1 << 2
        case .error:
            return 1 << 3
        }
    }
}

extension ExecutionLogFilterPreference: UserDefaultSettable {
    static func getKey() -> String {
        "com.zizicici.common.settings.ExecutionLogFilterPreference"
    }

    static func getHeader() -> String? {
        String(localized: "settings.logFilter.header")
    }

    static func getFooter() -> String? {
        String(localized: "settings.logFilter.footer")
    }

    func getName() -> String {
        let names = enabledLevels
            .sorted { $0.rawValue < $1.rawValue }
            .map { $0.rawValue.uppercased() }
        return names.isEmpty ? "None" : names.joined(separator: " / ")
    }

    static func getTitle() -> String {
        String(localized: "settings.logFilter.header")
    }

    static func getOptions() -> [ExecutionLogFilterPreference] {
        [defaultOption]
    }
}
