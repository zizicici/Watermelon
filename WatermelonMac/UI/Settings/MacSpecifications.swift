import Foundation
import MoreKit

enum MacSpecifications {
  @MainActor
  static func current(
    bundle: Bundle = .main
  ) -> SpecificationsConfiguration {
    SpecificationsConfiguration(
      summaryItems: [
        .init(
          type: .name,
          value: SpecificationsViewController.getAppName(
            bundle: bundle
          ) ?? "Watermelon Backup"
        ),
        .init(
          type: .version,
          value: SpecificationsViewController.getAppVersion(
            bundle: bundle
          ) ?? "—"
        ),
        .init(type: .manufacturer, value: "@App君"),
        .init(type: .publisher, value: "ZIZICICI LIMITED"),
        .init(type: .dateOfProduction, value: "2026/07/29"),
        .init(type: .license, value: "粤ICP备2025448771号-6A"),
      ],
      thirdPartyLibraries: [
        .init(
          name: "AMSMB2",
          version: "master",
          urlString: "https://github.com/zizicici/AMSMB2"
        ),
        .init(
          name: "Citadel",
          version: "fix/sftp-response-lock",
          urlString: "https://github.com/zizicici/Citadel"
        ),
        .init(
          name: "GRDB",
          version: "7.10.0",
          urlString: "https://github.com/groue/GRDB.swift"
        ),
        .init(
          name: "MoreKit",
          version: "codex/macos-support",
          urlString: "https://github.com/zizicici/MoreKit"
        ),
        .init(
          name: "MSAL",
          version: "2.11.0",
          urlString:
            "https://github.com/AzureAD/microsoft-authentication-library-for-objc"
        ),
      ]
    )
  }
}
