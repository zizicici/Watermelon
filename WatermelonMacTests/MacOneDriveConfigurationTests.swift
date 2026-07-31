import XCTest

@testable import WatermelonMac

final class MacOneDriveConfigurationTests: XCTestCase {
  func testRedirectURIUsesMacBundleIdentifier() throws {
    let bundleIdentifier = try XCTUnwrap(
      Bundle.main.bundleIdentifier
    )
    let redirectValue = try XCTUnwrap(
      Bundle.main.object(
        forInfoDictionaryKey: "OneDriveRedirectURI"
      ) as? String
    )
    let redirectURL = try XCTUnwrap(URL(string: redirectValue))

    XCTAssertEqual(
      redirectURL.scheme,
      "msauth.\(bundleIdentifier)"
    )
    XCTAssertEqual(redirectURL.host, "auth")
    XCTAssertTrue(
      registeredURLSchemes.contains(
        try XCTUnwrap(redirectURL.scheme)
      )
    )
  }

  func testMSALAcceptsMacApplicationConfiguration() {
    do {
      _ = try OneDriveMSALService()
        .cachedHomeAccountIdentifiers()
    } catch OneDriveAuthenticationError.configurationMissing {
      XCTFail("MSAL rejected the Mac application configuration")
    } catch {
      // Unsigned tests may not access the release Keychain group.
    }
  }

  private var registeredURLSchemes: [String] {
    let urlTypes =
      Bundle.main.object(
        forInfoDictionaryKey: "CFBundleURLTypes"
      ) as? [[String: Any]] ?? []
    return urlTypes.flatMap {
      $0["CFBundleURLSchemes"] as? [String] ?? []
    }
  }
}
