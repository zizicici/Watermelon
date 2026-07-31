import Foundation

enum BackupWorkerCountMode: Int, CaseIterable, Codable, Sendable {
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

    static var persistedValue: BackupWorkerCountMode {
        let key = "com.zizicici.common.settings.BackupWorkerCountMode"
        guard UserDefaults.standard.object(forKey: key) != nil,
              let value = BackupWorkerCountMode(
                rawValue: UserDefaults.standard.integer(forKey: key)
              ) else {
            return .automatic
        }
        return value
    }

    static func setPersistedValue(_ value: BackupWorkerCountMode) {
        UserDefaults.standard.set(
            value.rawValue,
            forKey: "com.zizicici.common.settings.BackupWorkerCountMode"
        )
    }

    var localizedText: String {
        guard let count = workerCountOverride else {
            return String(localized: "settings.worker.automatic")
        }
        return String.localizedStringWithFormat(
            String(localized: "settings.worker.count"),
            count
        )
    }
}

enum NodeBackupWorkerCountSelection: CaseIterable, Equatable, Sendable {
    case globalDefault
    case automatic
    case count(Int)

    static var allCases: [NodeBackupWorkerCountSelection] {
        [.globalDefault, .automatic]
            + ServerProfileRecord.allowedUploadWorkerCounts.map { .count($0) }
    }

    init(persistedMode: Int?) {
        guard let persistedMode else {
            self = .globalDefault
            return
        }
        if persistedMode == BackupWorkerCountMode.automatic.rawValue {
            self = .automatic
        } else if ServerProfileRecord.allowedUploadWorkerCounts.contains(
            persistedMode
        ) {
            self = .count(persistedMode)
        } else {
            self = .globalDefault
        }
    }

    var persistedMode: Int? {
        switch self {
        case .globalDefault:
            return nil
        case .automatic:
            return BackupWorkerCountMode.automatic.rawValue
        case .count(let count):
            return count
        }
    }

    func localizedText(
        globalDefault: BackupWorkerCountMode = .persistedValue
    ) -> String {
        switch self {
        case .globalDefault:
            return String.localizedStringWithFormat(
                String(
                    localized:
                        "settings.worker.node.useGlobalDefault"
                ),
                globalDefault.localizedText
            )
        case .automatic:
            return BackupWorkerCountMode.automatic.localizedText
        case .count(let count):
            return String.localizedStringWithFormat(
                String(localized: "settings.worker.count"),
                count
            )
        }
    }
}

enum BackupWorkerCountResolver {
    static func workerCountOverride(
        for profile: ServerProfileRecord,
        globalDefault: BackupWorkerCountMode = .persistedValue
    ) -> Int? {
        switch NodeBackupWorkerCountSelection(
            persistedMode: profile.uploadWorkerCountMode
        ) {
        case .globalDefault:
            return globalDefault.workerCountOverride
        case .automatic:
            return nil
        case .count(let count):
            return count
        }
    }
}

enum ICloudPhotoBackupMode: Int, CaseIterable, Codable, Sendable {
    case disable = 0
    case enable

    var allowsNetworkAccess: Bool {
        self == .enable
    }

    static var persistedValue: ICloudPhotoBackupMode {
        let key = "com.zizicici.common.settings.ICloudPhotoBackupMode"
        guard UserDefaults.standard.object(forKey: key) != nil,
              let value = ICloudPhotoBackupMode(
                rawValue: UserDefaults.standard.integer(forKey: key)
              ) else {
            return .disable
        }
        return value
    }

    static func setPersistedValue(_ value: ICloudPhotoBackupMode) {
        UserDefaults.standard.set(
            value.rawValue,
            forKey: "com.zizicici.common.settings.ICloudPhotoBackupMode"
        )
    }
}
