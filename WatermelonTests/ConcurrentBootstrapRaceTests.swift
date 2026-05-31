import XCTest
@testable import Watermelon

// Phase 2 removed ensureRepoJSON (claim election path). Concurrent bootstrap
// convergence is now handled by ensureIdentityFinalization's exclusive-create
// gate. Full concurrent-identity tests will be added in Phase 7.
