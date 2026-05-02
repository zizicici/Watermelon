import XCTest
@testable import Watermelon

final class S3SigV4SignerTests: XCTestCase {
    // AWS-published example credentials (only valid as test fixtures, never as real keys).
    private let accessKey = "AKIAIOSFODNN7EXAMPLE"
    private let secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    private let region = "us-east-1"

    private var fixedDate: Date {
        var components = DateComponents()
        components.year = 2013
        components.month = 5
        components.day = 24
        components.hour = 0
        components.minute = 0
        components.second = 0
        components.timeZone = TimeZone(secondsFromGMT: 0)
        return Calendar(identifier: .gregorian).date(from: components)!
    }

    /// AWS S3 SigV4 documented example: GET test.txt with a Range header.
    /// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    func testGetObjectWithRangeHeader() {
        let url = URL(string: "https://examplebucket.s3.amazonaws.com/test.txt")!

        let result = S3SigV4Signer.sign(
            method: "GET",
            url: url,
            additionalHeaders: ["Range": "bytes=0-9"],
            bodyHash: .empty,
            accessKeyID: accessKey,
            secretAccessKey: secretKey,
            region: region,
            date: fixedDate
        )

        let expectedCanonical = """
        GET
        /test.txt

        host:examplebucket.s3.amazonaws.com
        range:bytes=0-9
        x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        x-amz-date:20130524T000000Z

        host;range;x-amz-content-sha256;x-amz-date
        e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        """

        XCTAssertEqual(result.canonicalRequest, expectedCanonical)
        XCTAssertEqual(result.signature, "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
        XCTAssertEqual(result.amzDate, "20130524T000000Z")
        XCTAssertEqual(result.payloadHash, S3SigV4Signer.emptyPayloadSHA256)

        let expectedAuth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        XCTAssertEqual(result.headers["authorization"], expectedAuth)
    }

    /// AWS S3 SigV4 documented example: PUT object with a body and storage-class header.
    /// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    func testPutObjectWithBodyAndStorageClass() {
        let url = URL(string: "https://examplebucket.s3.amazonaws.com/test$file.text")!
        let body = Data("Welcome to Amazon S3.".utf8)

        let result = S3SigV4Signer.sign(
            method: "PUT",
            url: url,
            additionalHeaders: [
                "Date": "Fri, 24 May 2013 00:00:00 GMT",
                "x-amz-storage-class": "REDUCED_REDUNDANCY"
            ],
            bodyHash: .data(body),
            accessKeyID: accessKey,
            secretAccessKey: secretKey,
            region: region,
            date: fixedDate
        )

        let expectedCanonical = """
        PUT
        /test%24file.text

        date:Fri, 24 May 2013 00:00:00 GMT
        host:examplebucket.s3.amazonaws.com
        x-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072
        x-amz-date:20130524T000000Z
        x-amz-storage-class:REDUCED_REDUNDANCY

        date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class
        44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072
        """

        XCTAssertEqual(result.canonicalRequest, expectedCanonical)
        XCTAssertEqual(result.signature, "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd")
        XCTAssertEqual(result.payloadHash, "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072")
    }

    /// Query-string canonicalization: parameters must be sorted by name and percent-encoded.
    func testQueryStringSortedAndEncoded() {
        let url = URL(string: "https://examplebucket.s3.amazonaws.com/?prefix=somePrefix&max-keys=1000")!

        let result = S3SigV4Signer.sign(
            method: "GET",
            url: url,
            bodyHash: .empty,
            accessKeyID: accessKey,
            secretAccessKey: secretKey,
            region: region,
            date: fixedDate
        )

        let lines = result.canonicalRequest.components(separatedBy: "\n")
        XCTAssertEqual(lines[0], "GET")
        XCTAssertEqual(lines[1], "/")
        XCTAssertEqual(lines[2], "max-keys=1000&prefix=somePrefix")
    }

    /// Path encoding: per S3 SigV4, each path segment is URI-encoded once. Slash is preserved.
    func testPathEncodingPreservesSlashesAndEncodesSpecials() {
        let url = URL(string: "https://example.s3.amazonaws.com/folder/photo+name.jpg")!

        let result = S3SigV4Signer.sign(
            method: "GET",
            url: url,
            bodyHash: .empty,
            accessKeyID: accessKey,
            secretAccessKey: secretKey,
            region: region,
            date: fixedDate
        )

        let lines = result.canonicalRequest.components(separatedBy: "\n")
        XCTAssertEqual(lines[1], "/folder/photo%2Bname.jpg")
    }

    /// Empty body must hash to the well-known SHA256("") constant.
    func testEmptyPayloadHashConstant() {
        XCTAssertEqual(
            S3SigV4Signer.hashString(for: .empty),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
    }

    /// Unsigned payload literal must match the SigV4 sentinel string.
    func testUnsignedPayloadSentinel() {
        XCTAssertEqual(S3SigV4Signer.hashString(for: .unsigned), "UNSIGNED-PAYLOAD")
    }
}
