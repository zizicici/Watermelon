import Foundation

enum RatingPromptEligibility {
    static let minimumDatabaseAge: TimeInterval = 7 * 24 * 3600

    static func isEligible(
        databaseCreationDate: Date?,
        now: Date = Date()
    ) -> Bool {
        guard let databaseCreationDate else { return false }
        return now.timeIntervalSince(databaseCreationDate)
            >= minimumDatabaseAge
    }
}
