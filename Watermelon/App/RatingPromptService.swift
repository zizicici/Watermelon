import Foundation
import StoreKit
import UIKit

enum RatingPromptService {
    private static let eligibleInterval: TimeInterval = 7 * 24 * 3600

    @MainActor
    static func requestReviewIfEligible(in scene: UIWindowScene) {
        guard let creationDate = databaseCreationDate() else { return }
        guard Date().timeIntervalSince(creationDate) >= eligibleInterval else { return }
        AppStore.requestReview(in: scene)
    }

    private static func databaseCreationDate() -> Date? {
        let url = DatabaseManager.defaultDatabaseURL()
        let attrs = try? FileManager.default.attributesOfItem(atPath: url.path)
        return attrs?[.creationDate] as? Date
    }
}
