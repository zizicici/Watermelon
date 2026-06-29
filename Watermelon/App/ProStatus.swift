//
//  ProStatus.swift
//  Watermelon
//

import Foundation
import MoreKit

enum ProStatus {
    static let productID = "com.zizicici.watermelon.pro"
    static let membershipKey = "com.zizicici.watermelon.membership.lifetime"
    private static let legacyCacheKey = "com.zizicici.watermelon.pro.entitled"

    @MainActor
    static var isPro: Bool {
        User.shared.proTier() == .lifetime
    }

    static func migrateLegacyCacheIfNeeded() {
        let defaults = UserDefaults.standard
        if defaults.bool(forKey: legacyCacheKey),
           !defaults.bool(forKey: membershipKey) {
            defaults.set(true, forKey: membershipKey)
        }
        defaults.removeObject(forKey: legacyCacheKey)
    }

    /// Refresh through MoreKit before async/background gates; `.missing` still never downgrades.
    @discardableResult
    static func verifyEntitlement() async -> Bool {
        await Store.shared.updateCustomerProductStatus()
        return User.shared.proTier() == .lifetime
    }
}
