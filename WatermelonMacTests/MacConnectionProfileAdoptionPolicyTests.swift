import XCTest
@testable import WatermelonMac

final class MacConnectionProfileAdoptionPolicyTests: XCTestCase {
    func testAcceptedSFTPHostKeyCanReplaceConnectingSnapshot() throws {
        let current = try makeProfile(
            id: 7,
            basePath: "/photos",
            credentialRef: "sftp-credential",
            authMethod: .password,
            fingerprint: "old-key"
        )
        var updated = current
        updated.name = "Renamed while connecting"
        updated.connectionParams =
            try ServerProfileRecord.encodedConnectionParams(
                SFTPConnectionParams(
                    authMethod: .password,
                    hostKeyFingerprintSHA256: "new-key"
                )
            )

        XCTAssertTrue(
            MacConnectionProfileAdoptionPolicy
                .canAdoptDuringConnection(
                    current: current,
                    updated: updated
                )
        )
        XCTAssertFalse(
            current.hasSameRemoteDestination(as: updated)
        )
    }

    func testUnrelatedConnectionChangesCannotBeAdopted() throws {
        let current = try makeProfile(
            id: 7,
            basePath: "/photos",
            credentialRef: "sftp-credential",
            authMethod: .password,
            fingerprint: "old-key"
        )

        var changedPath = current
        changedPath.basePath = "/other"
        XCTAssertFalse(
            MacConnectionProfileAdoptionPolicy
                .canAdoptDuringConnection(
                    current: current,
                    updated: changedPath
                )
        )

        var changedCredential = current
        changedCredential.credentialRef = "other-credential"
        XCTAssertFalse(
            MacConnectionProfileAdoptionPolicy
                .canAdoptDuringConnection(
                    current: current,
                    updated: changedCredential
                )
        )

        var changedID = current
        changedID.id = 8
        XCTAssertFalse(
            MacConnectionProfileAdoptionPolicy
                .canAdoptDuringConnection(
                    current: current,
                    updated: changedID
                )
        )

        var changedAuth = current
        changedAuth.connectionParams =
            try ServerProfileRecord.encodedConnectionParams(
                SFTPConnectionParams(
                    authMethod: .privateKey,
                    hostKeyFingerprintSHA256: "old-key"
                )
            )
        XCTAssertFalse(
            MacConnectionProfileAdoptionPolicy
                .canAdoptDuringConnection(
                    current: current,
                    updated: changedAuth
                )
        )
    }

    private func makeProfile(
        id: Int64,
        basePath: String,
        credentialRef: String,
        authMethod: SFTPConnectionParams.AuthMethod,
        fingerprint: String
    ) throws -> ServerProfileRecord {
        ServerProfileRecord(
            id: id,
            name: "SFTP",
            storageType: StorageType.sftp.rawValue,
            connectionParams:
                try ServerProfileRecord.encodedConnectionParams(
                    SFTPConnectionParams(
                        authMethod: authMethod,
                        hostKeyFingerprintSHA256: fingerprint
                    )
                ),
            sortOrder: 0,
            host: "nas.local",
            port: 22,
            shareName: "",
            basePath: basePath,
            username: "user",
            domain: nil,
            credentialRef: credentialRef,
            createdAt: Date(),
            updatedAt: Date()
        )
    }
}
