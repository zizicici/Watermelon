import XCTest
@testable import Watermelon

final class S3SigV4SignerTests: XCTestCase {
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

    func testEmptyPayloadHashConstant() {
        XCTAssertEqual(
            S3SigV4Signer.hashString(for: .empty),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
    }

    func testUnsignedPayloadSentinel() {
        XCTAssertEqual(S3SigV4Signer.hashString(for: .unsigned), "UNSIGNED-PAYLOAD")
    }

    func testHostHeaderIncludesNonDefaultPort() {
        let url = URL(string: "http://minio.local:9000/bucket/test.bin")!
        let result = S3SigV4Signer.sign(
            method: "GET",
            url: url,
            bodyHash: .empty,
            accessKeyID: accessKey,
            secretAccessKey: secretKey,
            region: region,
            date: fixedDate
        )
        XCTAssertEqual(result.headers["host"], "minio.local:9000")
        let canonicalLines = result.canonicalRequest.components(separatedBy: "\n")
        XCTAssertTrue(canonicalLines.contains("host:minio.local:9000"),
                      "canonical request missing host:port; lines=\(canonicalLines)")
    }

    func testUnsignedPayloadIntegratesIntoSignedRequest() {
        let url = URL(string: "https://examplebucket.s3.amazonaws.com/big.bin?partNumber=1&uploadId=abc")!
        let result = S3SigV4Signer.sign(
            method: "PUT",
            url: url,
            bodyHash: .unsigned,
            accessKeyID: accessKey,
            secretAccessKey: secretKey,
            region: region,
            date: fixedDate
        )
        XCTAssertEqual(result.payloadHash, "UNSIGNED-PAYLOAD")
        XCTAssertEqual(result.headers["x-amz-content-sha256"], "UNSIGNED-PAYLOAD")
        XCTAssertTrue(result.canonicalRequest.hasSuffix("UNSIGNED-PAYLOAD"),
                      "canonical request must end with UNSIGNED-PAYLOAD; got \(result.canonicalRequest)")
    }
}
