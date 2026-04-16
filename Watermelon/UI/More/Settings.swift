//
//  Settings.swift
//  Watermelon
//
//  Created by zici on 2/5/24.
//

import Foundation
import MoreKit

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
        "上传并发 Worker"
    }

    static func getFooter() -> String? {
        """
        自动模式会按存储协议使用默认并发（SMB/WebDAV=2，本地存储=3）。
        手动模式会覆盖协议默认值。
        若启用“允许访问 iCloud 原件”且当前范围内检测到仅存于 iCloud 的资源，执行会自动改为 1 个 Worker。
        若当前已有进行中的备份任务，需要先停止并重新开始，新设置才会生效。
        """
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
        "允许访问 iCloud 原件"
    }

    static func getFooter() -> String? {
        """
        Enable 后允许 Watermelon 在备份、同步、下载前的去重过程中按需访问 iCloud 原件。
        若当前范围内检测到仅存于 iCloud 的资源，执行会自动改为 1 个 Worker。
        Disable 时仅处理已经在本机的资源。
        若当前已有进行中的备份任务，需要先停止并重新开始，新设置才会生效。
        """
    }

    func getName() -> String {
        switch self {
        case .disable:
            return "Disable"
        case .enable:
            return "Enable"
        }
    }

    static func getTitle() -> String {
        "允许访问 iCloud 原件"
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
        "执行日志筛选"
    }

    static func getFooter() -> String? {
        "保存执行日志页右上角筛选菜单的已选日志级别。"
    }

    func getName() -> String {
        let names = enabledLevels
            .sorted { $0.rawValue < $1.rawValue }
            .map { $0.rawValue.uppercased() }
        return names.isEmpty ? "None" : names.joined(separator: " / ")
    }

    static func getTitle() -> String {
        "执行日志筛选"
    }

    static func getOptions() -> [ExecutionLogFilterPreference] {
        [defaultOption]
    }
}
