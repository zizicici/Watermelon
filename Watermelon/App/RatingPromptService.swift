import Foundation
import StoreKit
import UIKit

enum RatingPromptService {
    @MainActor
    static func requestReviewIfEligible(in scene: UIWindowScene) {
        guard RatingPromptEligibility.isEligible(
            databaseCreationDate: databaseCreationDate()
        ) else {
            return
        }
        AppStore.requestReview(in: scene)
    }

    private static func databaseCreationDate() -> Date? {
        let url = DatabaseManager.defaultDatabaseURL()
        let attrs = try? FileManager.default.attributesOfItem(atPath: url.path)
        return attrs?[.creationDate] as? Date
    }
}
