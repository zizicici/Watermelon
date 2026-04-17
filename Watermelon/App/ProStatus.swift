//
//  ProStatus.swift
//  Watermelon
//

import MoreKit
import StoreKit

enum ProStatus {
    static let productID = "com.zizicici.watermelon.pro"
    private static let cacheKey = "com.zizicici.watermelon.pro.entitled"

    /// Synchronous check backed by UserDefaults cache.
    /// Safe on cold launch — the cache persists across launches and is updated
    /// by StoreKit notifications and the async verifyEntitlement() call.
    @MainActor
    static var isPro: Bool {
        if UserDefaults.standard.bool(forKey: cacheKey) {
            return true
        }
        return Store.shared.proTier() == .lifetime
    }

    /// Async check using StoreKit 2 local entitlement cache.
    /// Updates the UserDefaults cache so subsequent sync reads are accurate.
    @discardableResult
    static func verifyEntitlement() async -> Bool {
        for await result in Transaction.currentEntitlements {
            if case .verified(let transaction) = result,
               transaction.productID == productID,
               transaction.productType == .nonConsumable {
                UserDefaults.standard.set(true, forKey: cacheKey)
                return true
            }
        }
        UserDefaults.standard.set(false, forKey: cacheKey)
        return false
    }

    /// Observe MoreKit store notifications to keep the cache in sync.
    /// Call once from AppDelegate.didFinishLaunching before MoreKit.configure
    /// so the first StoreInfoLoaded notification isn't missed.
    static func setupStoreObserver() {
        NotificationCenter.default.addObserver(
            forName: .StoreInfoLoaded,
            object: nil,
            queue: .main
        ) { _ in
            MainActor.assumeIsolated {
                UserDefaults.standard.set(
                    Store.shared.hasValidMembership(),
                    forKey: cacheKey
                )
            }
        }
    }
}
