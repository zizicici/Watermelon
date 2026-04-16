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
