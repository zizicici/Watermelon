import XCTest
@testable import Watermelon

final class S3ClientTests: XCTestCase {
    private func makeClient(usePathStyle: Bool, host: String = "s3.us-east-1.amazonaws.com", port: Int = 0, scheme: String = "https") -> S3Client {
        S3Client(config: S3Client.Config(
            endpointHost: host,
            endpointPort: port,
            scheme: scheme,
            region: "us-east-1",
            bucket: "examplebucket",
            usePathStyle: usePathStyle,
            accessKeyID: "AKIAIOSFODNN7EXAMPLE",
            secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            sessionToken: nil
        ))
    }

    // MARK: - URL building

    func testVirtualHostedStyleURLForKey() throws {
        let client = makeClient(usePathStyle: false)
        let url = try client.makeURL(key: "photos/2026/01/IMG_0001.HEIC", query: [])
        XCTAssertEqual(url.absoluteString, "https://examplebucket.s3.us-east-1.amazonaws.com/photos/2026/01/IMG_0001.HEIC")
    }

    func testPathStyleURLForKey() throws {
        let client = makeClient(usePathStyle: true, host: "play.min.io", port: 9000, scheme: "http")
        let url = try client.makeURL(key: "photos/test.jpg", query: [])
        XCTAssertEqual(url.absoluteString, "http://play.min.io:9000/examplebucket/photos/test.jpg")
    }

    func testEmptyKeyHitsBucketRoot() throws {
        let virtual = makeClient(usePathStyle: false)
        XCTAssertEqual(try virtual.makeURL(key: "", query: []).absoluteString,
                       "https://examplebucket.s3.us-east-1.amazonaws.com/")
        let pathStyle = makeClient(usePathStyle: true)
        XCTAssertEqual(try pathStyle.makeURL(key: "", query: []).absoluteString,
                       "https://s3.us-east-1.amazonaws.com/examplebucket")
    }

    func testQueryStringPreservesEncodingForListObjectsV2() throws {
        let client = makeClient(usePathStyle: false)
        let url = try client.makeURL(key: "", query: [
            ("list-type", "2"),
            ("prefix", "photos/2026/"),
            ("delimiter", "/")
        ])
        XCTAssertEqual(url.absoluteString,
                       "https://examplebucket.s3.us-east-1.amazonaws.com/?list-type=2&prefix=photos%2F2026%2F&delimiter=%2F")
    }

    func testKeyWithSpecialCharactersIsPercentEncoded() throws {
        let client = makeClient(usePathStyle: false)
        let url = try client.makeURL(key: "photos/test file+name.jpg", query: [])
        XCTAssertEqual(url.absoluteString,
                       "https://examplebucket.s3.us-east-1.amazonaws.com/photos/test%20file%2Bname.jpg")
    }

    func testNonDefaultPortIsIncluded() throws {
        let client = makeClient(usePathStyle: true, host: "minio.local", port: 9000, scheme: "http")
        let url = try client.makeURL(key: "k", query: [])
        XCTAssertEqual(url.absoluteString, "http://minio.local:9000/examplebucket/k")
    }

    func testDefaultPortIsOmitted() throws {
        let client = makeClient(usePathStyle: false, port: 443)
        let url = try client.makeURL(key: "k", query: [])
        XCTAssertEqual(url.absoluteString, "https://examplebucket.s3.us-east-1.amazonaws.com/k")
    }

    // MARK: - ListObjectsV2 XML parsing

    func testListXMLParserExtractsContentsAndCommonPrefixes() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Name>examplebucket</Name>
          <Prefix>photos/</Prefix>
          <Delimiter>/</Delimiter>
          <KeyCount>2</KeyCount>
          <MaxKeys>1000</MaxKeys>
          <IsTruncated>false</IsTruncated>
          <Contents>
            <Key>photos/cover.jpg</Key>
            <LastModified>2026-01-15T12:34:56.000Z</LastModified>
            <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
            <Size>1234</Size>
            <StorageClass>STANDARD</StorageClass>
          </Contents>
          <CommonPrefixes>
            <Prefix>photos/2025/</Prefix>
          </CommonPrefixes>
          <CommonPrefixes>
            <Prefix>photos/2026/</Prefix>
          </CommonPrefixes>
        </ListBucketResult>
        """
        let result = try S3ListXMLParser().parse(data: Data(xml.utf8))
        XCTAssertEqual(result.contents.count, 1)
        XCTAssertEqual(result.contents[0].key, "photos/cover.jpg")
        XCTAssertEqual(result.contents[0].size, 1234)
        XCTAssertNotNil(result.contents[0].lastModified)
        XCTAssertEqual(result.commonPrefixes, ["photos/2025/", "photos/2026/"])
        XCTAssertFalse(result.isTruncated)
        XCTAssertNil(result.nextContinuationToken)
    }

    func testListXMLParserCapturesContinuationTokenWhenTruncated() throws {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
          <IsTruncated>true</IsTruncated>
          <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>
        </ListBucketResult>
        """
        let result = try S3ListXMLParser().parse(data: Data(xml.utf8))
        XCTAssertTrue(result.isTruncated)
        XCTAssertEqual(result.nextContinuationToken, "1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=")
    }

    func testListXMLParserIgnoresEchoedRequestPrefix() throws {
        // The top-level <Prefix>photos/</Prefix> is the echoed request parameter,
        // which must NOT appear in commonPrefixes.
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
          <Prefix>photos/</Prefix>
          <CommonPrefixes><Prefix>photos/2026/</Prefix></CommonPrefixes>
        </ListBucketResult>
        """
        let result = try S3ListXMLParser().parse(data: Data(xml.utf8))
        XCTAssertEqual(result.commonPrefixes, ["photos/2026/"])
    }

    // MARK: - S3 Error XML parsing

    func testErrorXMLParserExtractsCodeAndMessage() {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
          <Code>NoSuchBucket</Code>
          <Message>The specified bucket does not exist</Message>
          <BucketName>does-not-exist</BucketName>
          <RequestId>X</RequestId>
          <HostId>Y</HostId>
        </Error>
        """
        let parsed = S3ErrorXMLParser().parse(data: Data(xml.utf8))
        XCTAssertEqual(parsed?.code, "NoSuchBucket")
        XCTAssertEqual(parsed?.message, "The specified bucket does not exist")
    }

    func testErrorXMLParserReturnsNilForNonErrorPayload() {
        let xml = "<ListBucketResult><Name>x</Name></ListBucketResult>"
        XCTAssertNil(S3ErrorXMLParser().parse(data: Data(xml.utf8)))
    }

    // MARK: - Copy header encoding

    func testCopySourceHeaderEncodesBucketAndKey() {
        XCTAssertEqual(
            S3Client.copySourceHeader(bucket: "examplebucket", key: "photos/cover.jpg"),
            "/examplebucket/photos/cover.jpg"
        )
    }

    func testCopySourceHeaderPercentEncodesSpecialCharacters() {
        XCTAssertEqual(
            S3Client.copySourceHeader(bucket: "examplebucket", key: "photos/test file+name.jpg"),
            "/examplebucket/photos/test%20file%2Bname.jpg"
        )
    }

    func testCopySourceHeaderPreservesPathSeparators() {
        XCTAssertEqual(
            S3Client.copySourceHeader(bucket: "my-bucket", key: "a/b/c/d.bin"),
            "/my-bucket/a/b/c/d.bin"
        )
    }

    // MARK: - Streaming SHA256

    func testStreamingSHA256MatchesInMemoryHashForSmallFile() throws {
        let content = Data("Welcome to Amazon S3.".utf8)
        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try content.write(to: tempURL)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let streamed = try S3SigV4Signer.sha256Hex(streamingFrom: tempURL)
        let inMemory = S3SigV4Signer.sha256Hex(data: content)
        XCTAssertEqual(streamed, inMemory)
        // AWS docs vector: SHA256("Welcome to Amazon S3.")
        XCTAssertEqual(streamed, "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072")
    }

    func testStreamingSHA256OfEmptyFileMatchesEmptyConstant() throws {
        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data().write(to: tempURL)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let streamed = try S3SigV4Signer.sha256Hex(streamingFrom: tempURL)
        XCTAssertEqual(streamed, S3SigV4Signer.emptyPayloadSHA256)
    }

    func testStreamingSHA256AcrossMultipleChunks() throws {
        // Write a 16 MiB + 1 byte file so the streaming hasher reads at least 3 chunks.
        let chunk = Data(repeating: 0x41, count: 8 * 1024 * 1024)
        let tail = Data([0x42])
        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try (chunk + chunk + tail).write(to: tempURL)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let streamed = try S3SigV4Signer.sha256Hex(streamingFrom: tempURL)
        let inMemory = S3SigV4Signer.sha256Hex(data: try Data(contentsOf: tempURL))
        XCTAssertEqual(streamed, inMemory)
    }

    // MARK: - Multipart helpers

    func testSimpleXMLValueParserExtractsUploadId() {
        let xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Bucket>examplebucket</Bucket>
          <Key>photos/big.mov</Key>
          <UploadId>VXBsb2FkSWRBYmM=</UploadId>
        </InitiateMultipartUploadResult>
        """
        let parsed = S3SimpleXMLValueParser(target: "UploadId").parse(data: Data(xml.utf8))
        XCTAssertEqual(parsed, "VXBsb2FkSWRBYmM=")
    }

    func testSimpleXMLValueParserReturnsNilForMissingTarget() {
        let xml = "<Foo><Bar>baz</Bar></Foo>"
        XCTAssertNil(S3SimpleXMLValueParser(target: "UploadId").parse(data: Data(xml.utf8)))
    }

    func testSimpleXMLValueParserReturnsFirstMatchOnly() {
        let xml = "<Root><Tag>first</Tag><Other><Tag>second</Tag></Other></Root>"
        XCTAssertEqual(S3SimpleXMLValueParser(target: "Tag").parse(data: Data(xml.utf8)), "first")
    }

    func testCompleteMultipartXMLContainsPartsInOrder() {
        let parts: [S3Client.UploadedPart] = [
            .init(partNumber: 1, etag: "\"abc\"", size: 1024),
            .init(partNumber: 2, etag: "\"def\"", size: 2048),
            .init(partNumber: 3, etag: "\"ghi\"", size: 512)
        ]
        let xml = S3Client.buildCompleteMultipartXML(parts: parts)
        XCTAssertEqual(xml, "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"abc\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"def\"</ETag></Part><Part><PartNumber>3</PartNumber><ETag>\"ghi\"</ETag></Part></CompleteMultipartUpload>")
    }

    func testCompleteMultipartXMLEmptyPartsListIsValid() {
        let xml = S3Client.buildCompleteMultipartXML(parts: [])
        XCTAssertEqual(xml, "<CompleteMultipartUpload></CompleteMultipartUpload>")
    }

    func testMultipartThresholdConstantsMatchAWSBounds() {
        // 5 MiB minimum part size (AWS hard floor for non-final parts)
        XCTAssertGreaterThanOrEqual(S3Client.multipartPartSize, 5 * 1024 * 1024)
        // Threshold should equal part size so single-vs-multipart is a clean cutover
        XCTAssertEqual(S3Client.multipartThreshold, S3Client.multipartPartSize)
    }

    func testPartSizeUsesBaselineForFilesUnderMaxAtBaseline() {
        // 8 MiB × 9000 ≈ 72 GiB — anything below stays at baseline
        let oneMiB: Int64 = 1024 * 1024
        XCTAssertEqual(S3Client.partSize(forFileSize: 0), 8 * oneMiB)
        XCTAssertEqual(S3Client.partSize(forFileSize: 100 * oneMiB), 8 * oneMiB)
        XCTAssertEqual(S3Client.partSize(forFileSize: 50 * 1024 * oneMiB), 8 * oneMiB)
    }

    func testPartSizeGrowsForFilesAboveTargetCount() {
        let oneMiB: Int64 = 1024 * 1024
        // 100 GiB / 9000 ≈ 11.4 MiB → round up to 16 MiB (next 8-MiB multiple)
        XCTAssertEqual(S3Client.partSize(forFileSize: 100 * 1024 * oneMiB), 16 * oneMiB)
        // 1 TiB / 9000 ≈ 116.5 MiB → round up to 120 MiB
        XCTAssertEqual(S3Client.partSize(forFileSize: 1024 * 1024 * oneMiB), 120 * oneMiB)
    }

    func testPartSizeKeepsTotalPartsBelowAWSCeiling() {
        let testSizes: [Int64] = [
            5 * 1024 * 1024 * 1024,
            80 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
            5 * 1024 * 1024 * 1024 * 1024
        ]
        for size in testSizes {
            let part = S3Client.partSize(forFileSize: size)
            let parts = (size + part - 1) / part
            XCTAssertLessThanOrEqual(parts, 10_000, "size=\(size) part=\(part) parts=\(parts)")
            XCTAssertGreaterThanOrEqual(part, 5 * 1024 * 1024, "part below AWS minimum at size=\(size)")
        }
    }
}
