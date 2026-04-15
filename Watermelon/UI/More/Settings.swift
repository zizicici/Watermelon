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
