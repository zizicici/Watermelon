//
//  ProStatus.swift
//  Watermelon
//

import Foundation
import MoreKit

enum ProStatus {
    static let freeMaximumStorageNodes = 1
    static let productID = "com.zizicici.watermelon.pro"
    static let monthlyProductID = "com.zizicici.watermelon.subscription.monthly"
    static let yearlyProductID = "com.zizicici.watermelon.subscription.yearly"
    static let subscriptionProductIDs = [monthlyProductID, yearlyProductID]
    static let displayProductIDs = subscriptionProductIDs + [productID]
    static let membershipKey = "com.zizicici.watermelon.membership.lifetime"
    private static let legacyCacheKey = "com.zizicici.watermelon.pro.entitled"

    @MainActor
    static var isPro: Bool {
        Store.shared.hasValidMembership()
    }

    static func migrateLegacyCacheIfNeeded() {
        let defaults = UserDefaults.standard
        if defaults.bool(forKey: legacyCacheKey),
           !defaults.bool(forKey: membershipKey) {
            defaults.set(true, forKey: membershipKey)
        }
        defaults.removeObject(forKey: legacyCacheKey)
    }

    @discardableResult
    static func verifyEntitlement() async -> Bool {
        await Store.shared.updateCustomerProductStatus()
        return Store.shared.hasValidMembership()
    }
}
