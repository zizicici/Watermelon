import AppKit
import Foundation
import StoreKit

enum MacRatingPromptService {
    @MainActor
    static func requestReviewIfEligible(
        in viewController: NSViewController
    ) {
        guard RatingPromptEligibility.isEligible(
            databaseCreationDate: databaseCreationDate()
        ) else {
            return
        }
        AppStore.requestReview(in: viewController)
    }

    private static func databaseCreationDate() -> Date? {
        let url = DatabaseManager.defaultDatabaseURL()
        let attributes = try? FileManager.default
            .attributesOfItem(atPath: url.path)
        return attributes?[.creationDate] as? Date
    }
}
