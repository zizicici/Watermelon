import XCTest
@testable import Watermelon

final class S3ClientTests: XCTestCase {
    override func tearDown() {
        S3MockURLProtocol.handler = nil
        super.tearDown()
    }

    private func makeClient(
        usePathStyle: Bool,
        host: String = "s3.us-east-1.amazonaws.com",
        port: Int = 0,
        scheme: String = "https",
        basePath: String = "/",
        region: String = "us-east-1",
        provider: S3ProviderSelection = .automatic,
        sessionConfiguration: URLSessionConfiguration? = nil,
        verificationCleanupRetryDelays: [TimeInterval] = RemoteProbeCleanupCoordinator.defaultRetryDelays
    ) -> S3Client {
        S3Client(config: S3Client.Config(
            endpointHost: host,
            endpointPort: port,
            scheme: scheme,
            region: region,
            bucket: "examplebucket",
            basePath: basePath,
            usePathStyle: usePathStyle,
            provider: provider,
            accessKeyID: "AKIAIOSFODNN7EXAMPLE",
            secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            sessionToken: nil
        ), sessionConfiguration: sessionConfiguration, verificationCleanupRetryDelays: verificationCleanupRetryDelays)
    }

    private func makeMockSessionConfiguration() -> URLSessionConfiguration {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [S3MockURLProtocol.self]
        return configuration
    }

    private func makeTemporaryUploadFile(_ contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(contents.utf8).write(to: url)
        return url
    }

    private func assertAliyunVerifierRequests(
        _ requests: [URLRequest],
        expectedHost: String
    ) throws {
        let listRequest = try XCTUnwrap(requests.first {
            guard $0.httpMethod == "GET",
                  let url = $0.url,
                  let items = URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems else {
                return false
            }
            return items.contains(URLQueryItem(name: "list-type", value: "2"))
                && items.contains(URLQueryItem(name: "max-keys", value: "1"))
        })
        XCTAssertEqual(listRequest.url?.host, expectedHost)
        let listAuthorization = try XCTUnwrap(listRequest.value(forHTTPHeaderField: "Authorization")).lowercased()
        XCTAssertTrue(listAuthorization.contains("/cn-hangzhou/s3/aws4_request"))

        let conditionalPuts = requests.filter {
            $0.httpMethod == "PUT"
                && $0.value(forHTTPHeaderField: "x-oss-forbid-overwrite") == "true"
        }
        XCTAssertEqual(conditionalPuts.count, 2)
        XCTAssertEqual(Set(conditionalPuts.compactMap(\.url?.path)).count, 1)

        let sourcePath = try XCTUnwrap(conditionalPuts.first?.url?.path)
        let copyRequest = try XCTUnwrap(requests.first {
            $0.httpMethod == "PUT" && $0.value(forHTTPHeaderField: "x-amz-copy-source") != nil
        })
        XCTAssertEqual(copyRequest.value(forHTTPHeaderField: "x-amz-copy-source"), "/examplebucket\(sourcePath)")
        XCTAssertEqual(copyRequest.url?.host, expectedHost)
        let destinationPath = try XCTUnwrap(copyRequest.url?.path)
        XCTAssertNotEqual(destinationPath, sourcePath)
        let authorization = try XCTUnwrap(copyRequest.value(forHTTPHeaderField: "Authorization")).lowercased()
        XCTAssertTrue(authorization.contains("x-amz-copy-source"))
        XCTAssertTrue(authorization.contains("/cn-hangzhou/s3/aws4_request"))
        let objectGets = requests.filter { $0.httpMethod == "GET" && $0.url?.query == nil }
        XCTAssertEqual(Set(objectGets.compactMap(\.url?.path)), [sourcePath, destinationPath])
        let deletes = requests.filter { $0.httpMethod == "DELETE" }
        XCTAssertEqual(Set(deletes.compactMap(\.url?.path)), [sourcePath, destinationPath])
        XCTAssertTrue(requests.allSatisfy { $0.url?.host == expectedHost })
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

    func testIPv6PathStyleURLUsesBracketedAuthority() throws {
        let client = makeClient(usePathStyle: true, host: "2001:0db8:0:0:0:0:0:1", port: 9000, scheme: "http")
        let url = try client.makeURL(key: "k", query: [])
        XCTAssertEqual(url.absoluteString, "http://[2001:db8::1]:9000/examplebucket/k")
        XCTAssertEqual(url.host, "2001:db8::1")
    }

    func testZonedIPv6PathStyleURLUsesEncodedZone() throws {
        let client = makeClient(usePathStyle: true, host: "[fe80::1%25en0]", port: 9000, scheme: "http")
        let url = try client.makeURL(key: "k", query: [])
        XCTAssertEqual(url.absoluteString, "http://[fe80::1%25en0]:9000/examplebucket/k")
        XCTAssertEqual(RemoteHostEndpoint.socketHost(url.host ?? ""), "fe80::1%en0")
    }

    func testVirtualHostedStyleRejectsIPLiteral() {
        let client = makeClient(usePathStyle: false, host: "2001:db8::1")
        XCTAssertThrowsError(try client.makeURL(key: "k", query: [])) { error in
            guard case RemoteStorageClientError.invalidConfiguration = error else {
                return XCTFail("Expected invalidConfiguration, got \(error)")
            }
        }
    }

    func testRootDotAndCanonicalHostsProduceSameOperationalURL() throws {
        let rootedPathStyle = makeClient(usePathStyle: true, host: "minio.local.", port: 9000, scheme: "http")
        let canonicalPathStyle = makeClient(usePathStyle: true, host: "minio.local", port: 9000, scheme: "http")
        XCTAssertEqual(
            try rootedPathStyle.makeURL(key: "k", query: []),
            try canonicalPathStyle.makeURL(key: "k", query: [])
        )

        let rootedVirtual = makeClient(usePathStyle: false, host: "s3.us-east-1.amazonaws.com.")
        let canonicalVirtual = makeClient(usePathStyle: false, host: "s3.us-east-1.amazonaws.com")
        XCTAssertEqual(
            try rootedVirtual.makeURL(key: "k", query: []),
            try canonicalVirtual.makeURL(key: "k", query: [])
        )
        XCTAssertNotEqual(
            try rootedPathStyle.makeURL(key: "k", query: []),
            try makeClient(usePathStyle: true, host: "other.local", port: 9000, scheme: "http")
                .makeURL(key: "k", query: [])
        )
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

    // MARK: - ListObjectsV2 pagination termination

    func testListContinuationContinuesWhenTruncatedWithToken() throws {
        let token = try S3Client.nextListContinuationToken(isTruncated: true, nextContinuationToken: "abc=")
        XCTAssertEqual(token, "abc=")
    }

    func testListContinuationStopsWhenNotTruncated() throws {
        XCTAssertNil(try S3Client.nextListContinuationToken(isTruncated: false, nextContinuationToken: nil))
    }

    func testListContinuationIgnoresStrayTokenWhenNotTruncated() throws {
        XCTAssertNil(try S3Client.nextListContinuationToken(isTruncated: false, nextContinuationToken: "abc="))
    }

    func testListContinuationFailsClosedWhenTruncatedWithoutToken() {
        XCTAssertThrowsError(try S3Client.nextListContinuationToken(isTruncated: true, nextContinuationToken: nil)) { error in
            // Must not be read as object absence, or the Lite data-directory probe would collapse a partial
            // listing to an empty directory and prune still-present remote objects from the month manifest.
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
            XCTAssertEqual(RemoteFaultLite.classify(error), .terminal)
        }
    }

    func testListContinuationFailsClosedWhenTruncatedWithEmptyToken() {
        XCTAssertThrowsError(try S3Client.nextListContinuationToken(isTruncated: true, nextContinuationToken: "")) { error in
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
        }
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

    // MARK: - Endpoint parsing

    func testParseEndpointFromBareHostDefaultsToHTTPS() {
        let parsed = S3Client.parseEndpoint("s3.amazonaws.com")
        XCTAssertEqual(parsed?.scheme, "https")
        XCTAssertEqual(parsed?.host, "s3.amazonaws.com")
        XCTAssertEqual(parsed?.port, 443)
    }

    func testParseEndpointFromHTTPSURLOmitsExplicitPort() {
        let parsed = S3Client.parseEndpoint("https://s3.eu-west-2.amazonaws.com")
        XCTAssertEqual(parsed?.scheme, "https")
        XCTAssertEqual(parsed?.host, "s3.eu-west-2.amazonaws.com")
        XCTAssertEqual(parsed?.port, 443)
    }

    func testParseEndpointFromHTTPURLWithExplicitPort() {
        let parsed = S3Client.parseEndpoint("http://minio.local:9000")
        XCTAssertEqual(parsed?.scheme, "http")
        XCTAssertEqual(parsed?.host, "minio.local")
        XCTAssertEqual(parsed?.port, 9000)
    }

    func testParseEndpointRejectsNonHTTPSchemes() {
        XCTAssertNil(S3Client.parseEndpoint("ftp://example.com"))
        XCTAssertNil(S3Client.parseEndpoint("s3://bucket"))
    }

    func testParseEndpointRejectsEmptyOrWhitespace() {
        XCTAssertNil(S3Client.parseEndpoint(""))
        XCTAssertNil(S3Client.parseEndpoint("   "))
    }

    func testParseEndpointRejectsNonRootURLComponents() {
        XCTAssertNil(S3Client.parseEndpoint("https://example.com/path"))
        XCTAssertNil(S3Client.parseEndpoint("https://example.com?query=value"))
        XCTAssertNil(S3Client.parseEndpoint("https://example.com#fragment"))
        XCTAssertNil(S3Client.parseEndpoint("https://user@example.com"))
        XCTAssertNil(S3Client.parseEndpoint("https://user:password@example.com"))
        XCTAssertNotNil(S3Client.parseEndpoint("https://example.com/"))
    }

    func testParseEndpointRejectsOutOfRangeExplicitPorts() {
        XCTAssertNil(S3Client.parseEndpoint("https://example.com:"))
        XCTAssertNil(S3Client.parseEndpoint("example.com:"))
        XCTAssertNil(S3Client.parseEndpoint("https://[::1]:"))
        XCTAssertNil(S3Client.parseEndpoint("https://example.com:0"))
        XCTAssertNil(S3Client.parseEndpoint("https://example.com:65536"))
        XCTAssertNil(S3Client.parseEndpoint("https://example.com:99999"))
        XCTAssertEqual(S3Client.parseEndpoint("https://example.com:1")?.port, 1)
        XCTAssertEqual(S3Client.parseEndpoint("https://example.com:65535")?.port, 65535)
    }

    func testParseStructuredEndpointAcceptsLegacyDefaultPortSentinel() {
        let parsed = S3Client.parseEndpoint(scheme: "https", host: "[2001:db8::1]", port: 0)
        XCTAssertEqual(parsed?.scheme, "https")
        XCTAssertEqual(parsed?.host, "2001:db8::1")
        XCTAssertEqual(parsed?.port, 443)
        let emptyScheme = S3Client.parseEndpoint(scheme: "  ", host: "objects.example.com", port: 0)
        XCTAssertEqual(emptyScheme?.scheme, "https")
        XCTAssertEqual(emptyScheme?.port, 443)
    }

    func testParseStructuredEndpointRejectsInvalidPersistedShape() {
        XCTAssertNil(S3Client.parseEndpoint(scheme: "ftp", host: "example.com", port: 21))
        XCTAssertNil(S3Client.parseEndpoint(scheme: "https", host: "example.com/path", port: 443))
        XCTAssertNil(S3Client.parseEndpoint(scheme: "https", host: "example.com", port: 65536))
        XCTAssertNil(S3Client.parseEndpoint(scheme: "ftp", host: "example.com", port: 443))
    }

    func testParseEndpointTrimsSurroundingWhitespace() {
        let parsed = S3Client.parseEndpoint("  https://s3.amazonaws.com  ")
        XCTAssertEqual(parsed?.host, "s3.amazonaws.com")
        XCTAssertEqual(parsed?.scheme, "https")
    }

    func testParseIPv6EndpointReturnsSocketHostWithoutBrackets() {
        let parsed = S3Client.parseEndpoint("http://[2001:0db8:0:0:0:0:0:1]:9000")
        XCTAssertEqual(parsed?.scheme, "http")
        XCTAssertEqual(parsed?.host, "2001:db8::1")
        XCTAssertEqual(parsed?.port, 9000)

        let zoned = S3Client.parseEndpoint("https://[fe80::1%25en0]")
        XCTAssertEqual(zoned?.host, "fe80::1%en0")
        XCTAssertEqual(zoned?.port, 443)
    }

    // MARK: - Path-style auto-detection

    func testDefaultPathStyleForAWSHostsIsVirtualHosted() {
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "s3.amazonaws.com"))
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "s3.us-east-1.amazonaws.com"))
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "s3.us-east-1.amazonaws.com."))
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "S3.AMAZONAWS.COM"))
    }

    func testDefaultPathStyleForR2IsVirtualHosted() {
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "abc123.r2.cloudflarestorage.com"))
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "abc123.r2.cloudflarestorage.com."))
    }

    func testDefaultPathStyleForB2IsVirtualHosted() {
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "s3.us-west-002.backblazeb2.com"))
    }

    func testDefaultPathStyleForAliyunOSSIsVirtualHosted() {
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "oss-cn-hangzhou.aliyuncs.com"))
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "s3.oss-cn-hangzhou.aliyuncs.com"))
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "oss-cn-hangzhou-internal.aliyuncs.com."))
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "CN-HANGZHOU.OSS.ALIYUNCS.COM"))
    }

    func testAliyunOfficialEndpointForcesVirtualHostedAddressing() throws {
        let client = makeClient(
            usePathStyle: true,
            host: "oss-cn-hangzhou.aliyuncs.com"
        )
        XCTAssertEqual(
            try client.makeURL(key: "locks/test.lock", query: []).absoluteString,
            "https://examplebucket.oss-cn-hangzhou.aliyuncs.com/locks/test.lock"
        )
    }

    func testAliyunCanonicalConnectionNormalizesPathStyle() throws {
        let pathStyleInput = try CanonicalS3Connection(
            scheme: "https",
            host: "oss-cn-hangzhou.aliyuncs.com",
            port: 443,
            region: "cn-hangzhou",
            usePathStyle: true,
            provider: .automatic,
            bucket: "examplebucket",
            basePath: "/Watermelon",
            accessKeyID: "access-key"
        )
        let current = try CanonicalS3Connection(
            scheme: "https",
            host: "oss-cn-hangzhou.aliyuncs.com",
            port: 443,
            region: "cn-hangzhou",
            usePathStyle: false,
            provider: .automatic,
            bucket: "examplebucket",
            basePath: "/Watermelon",
            accessKeyID: "access-key"
        )

        XCTAssertFalse(pathStyleInput.usePathStyle)
        XCTAssertEqual(pathStyleInput, current)
        XCTAssertEqual(pathStyleInput.publishedV2IdentityComponents, current.publishedV2IdentityComponents)
    }

    func testAliyunOSSDetectionRejectsUnrelatedAndForgedHosts() {
        XCTAssertFalse(S3Canonicalization.isAliyunOSSEndpointHost("ecs.aliyuncs.com"))
        XCTAssertFalse(S3Canonicalization.isAliyunOSSEndpointHost("evilaliyuncs.com"))
        XCTAssertFalse(S3Canonicalization.isAliyunOSSEndpointHost("oss-cn-hangzhou.aliyuncs.com.example"))
        XCTAssertFalse(S3Canonicalization.isAliyunOSSEndpointHost("aliyuncs.com"))
        XCTAssertFalse(S3Canonicalization.isAliyunOSSEndpointHost("examplebucket.oss-cn-hangzhou.aliyuncs.com"))
    }

    func testAliyunBucketDomainsAreDetected() {
        XCTAssertTrue(S3Canonicalization.isAliyunOSSBucketDomain("examplebucket.oss-cn-hangzhou.aliyuncs.com"))
        XCTAssertTrue(S3Canonicalization.isAliyunOSSBucketDomain("examplebucket.s3.oss-cn-hangzhou.aliyuncs.com"))
        XCTAssertTrue(S3Canonicalization.isAliyunOSSBucketDomain("examplebucket.cn-hangzhou.oss.aliyuncs.com"))
        XCTAssertFalse(S3Canonicalization.isAliyunOSSBucketDomain("oss-cn-hangzhou.aliyuncs.com"))
        XCTAssertFalse(S3Canonicalization.isAliyunOSSBucketDomain("s3.oss-cn-hangzhou.aliyuncs.com"))
        XCTAssertFalse(S3Canonicalization.isAliyunOSSBucketDomain("cn-hangzhou.oss.aliyuncs.com"))
    }

    func testAliyunBucketDomainIsUsedWithoutAddingBucketAgain() throws {
        let client = makeClient(
            usePathStyle: false,
            host: "examplebucket.oss-cn-hangzhou.aliyuncs.com",
            region: "cn-hangzhou"
        )
        XCTAssertEqual(
            try client.makeURL(key: "locks/test.lock", query: []).absoluteString,
            "https://examplebucket.oss-cn-hangzhou.aliyuncs.com/locks/test.lock"
        )
    }

    func testAliyunBucketDomainMustMatchConfiguredBucket() throws {
        let connection = try CanonicalS3Connection(
            scheme: "https",
            host: "examplebucket.oss-cn-hangzhou.aliyuncs.com",
            port: 443,
            region: "cn-hangzhou",
            usePathStyle: false,
            provider: .automatic,
            bucket: "examplebucket",
            basePath: "/Watermelon",
            accessKeyID: "access-key"
        )
        XCTAssertTrue(connection.endpointIncludesBucket)

        XCTAssertThrowsError(try CanonicalS3Connection(
            scheme: "https",
            host: "examplebucket.oss-cn-hangzhou.aliyuncs.com",
            port: 443,
            region: "cn-hangzhou",
            usePathStyle: false,
            provider: .automatic,
            bucket: "anotherbucket",
            basePath: "/Watermelon",
            accessKeyID: "access-key"
        ))
    }

    func testAliyunCNAMEProviderUsesBucketBoundEndpoint() throws {
        let client = makeClient(
            usePathStyle: true,
            host: "backup.example.com",
            region: "cn-hangzhou",
            provider: .aliyunOSS
        )
        XCTAssertEqual(
            try client.makeURL(key: "locks/test.lock", query: []).absoluteString,
            "https://backup.example.com/locks/test.lock"
        )
    }

    func testAliyunCNAMECanonicalConnectionKeepsProviderAndDirectDisplayURL() throws {
        let connection = try CanonicalS3Connection(
            scheme: "https",
            host: "backup.example.com",
            port: 443,
            region: "cn-hangzhou",
            usePathStyle: true,
            provider: .aliyunOSS,
            bucket: "examplebucket",
            basePath: "/Watermelon",
            accessKeyID: "access-key"
        )

        XCTAssertTrue(connection.endpointIncludesBucket)
        XCTAssertFalse(connection.usePathStyle)
        XCTAssertEqual(connection.provider, .aliyunOSS)
        XCTAssertEqual(
            CanonicalProfileConnection.s3(connection).displaySubtitle,
            "https://backup.example.com/Watermelon"
        )
        XCTAssertEqual(connection.publishedV2IdentityComponents[4], "aliyun-bucket-bound")
    }

    func testS3ConnectionParamsPreserveSelectedProvider() throws {
        let original = S3ConnectionParams(
            scheme: "HTTPS",
            region: "cn-hangzhou",
            usePathStyle: false,
            provider: .aliyunOSS
        )
        let decoded = try JSONDecoder().decode(
            S3ConnectionParams.self,
            from: JSONEncoder().encode(original)
        )

        XCTAssertEqual(decoded.scheme, "https")
        XCTAssertEqual(decoded.region, "cn-hangzhou")
        XCTAssertFalse(decoded.usePathStyle)
        XCTAssertEqual(decoded.provider, .aliyunOSS)
    }

    func testS3ConnectionParamsDefaultMissingProviderForStoredProfiles() throws {
        let data = Data(#"{"scheme":"https","region":"us-east-1","usePathStyle":false}"#.utf8)
        let decoded = try JSONDecoder().decode(S3ConnectionParams.self, from: data)

        XCTAssertEqual(decoded.scheme, "https")
        XCTAssertEqual(decoded.region, "us-east-1")
        XCTAssertFalse(decoded.usePathStyle)
        XCTAssertEqual(decoded.provider, .automatic)

        let reencoded = try JSONEncoder().encode(decoded)
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: reencoded) as? [String: Any])
        XCTAssertEqual(object["provider"] as? String, S3ProviderSelection.automatic.rawValue)

        let profile = ServerProfileRecord(
            id: 1,
            name: "Stored S3",
            storageType: StorageType.s3.rawValue,
            connectionParams: data,
            sortOrder: 0,
            host: "s3.us-east-1.amazonaws.com",
            port: 443,
            shareName: "examplebucket",
            basePath: "/Watermelon",
            username: "access-key",
            domain: nil,
            credentialRef: "stored-s3",
            createdAt: Date(),
            updatedAt: Date()
        )
        guard case .s3(let connection) = profile.canonicalConnection else {
            return XCTFail("Expected the stored S3 profile to remain canonicalizable")
        }
        XCTAssertEqual(connection.provider, .automatic)
    }

    func testS3ConnectionParamsRejectExplicitNullProvider() {
        let data = Data(#"{"scheme":"https","region":"us-east-1","usePathStyle":false,"provider":null}"#.utf8)
        XCTAssertThrowsError(try JSONDecoder().decode(S3ConnectionParams.self, from: data))
    }

    func testDefaultProviderKeepsCustomEndpointOnStandardS3Addressing() throws {
        let client = makeClient(
            usePathStyle: true,
            host: "backup.example.com",
            region: "cn-hangzhou"
        )
        XCTAssertEqual(
            try client.makeURL(key: "locks/test.lock", query: []).absoluteString,
            "https://backup.example.com/examplebucket/locks/test.lock"
        )
    }

    func testAliyunOSSRegionMustBeExplicit() {
        XCTAssertNil(S3Client.defaultRegion(forHost: "oss-cn-hangzhou.aliyuncs.com"))
        XCTAssertNil(S3Client.defaultRegion(forHost: "oss-cn-hangzhou-internal.aliyuncs.com"))
        XCTAssertNil(S3Client.defaultRegion(forHost: "s3.oss-ap-southeast-1.aliyuncs.com"))
        XCTAssertNil(S3Client.defaultRegion(forHost: "CN-HANGZHOU.OSS.ALIYUNCS.COM"))
        XCTAssertNil(S3Client.defaultRegion(forHost: "oss-accelerate.aliyuncs.com"))
        XCTAssertTrue(S3Canonicalization.usesAliyunOSS(provider: .automatic, host: "oss-accelerate.aliyuncs.com"))
        XCTAssertEqual(
            S3Client.resolveRegion(userInput: "", host: "oss-cn-hangzhou.aliyuncs.com"),
            ""
        )
        XCTAssertEqual(
            S3Client.resolveRegion(userInput: " cn-hangzhou ", host: "oss-cn-hangzhou.aliyuncs.com"),
            "cn-hangzhou"
        )
    }

    func testCanonicalAliyunConnectionsRejectEmptyRegion() {
        for (host, provider) in [
            ("oss-cn-hangzhou.aliyuncs.com", S3ProviderSelection.automatic),
            ("examplebucket.oss-cn-hangzhou.aliyuncs.com", .automatic),
            ("backup.example.com", .aliyunOSS)
        ] {
            XCTAssertThrowsError(try CanonicalS3Connection(
                scheme: "https",
                host: host,
                port: 443,
                region: "",
                usePathStyle: false,
                provider: provider,
                bucket: "examplebucket",
                basePath: "/Watermelon",
                accessKeyID: "access-key"
            ))
        }
    }

    func testDefaultPathStyleForUnknownHostsIsPathStyle() {
        XCTAssertTrue(S3Client.defaultPathStyle(forHost: "minio.local"))
        XCTAssertTrue(S3Client.defaultPathStyle(forHost: "play.min.io"))
        XCTAssertTrue(S3Client.defaultPathStyle(forHost: "192.168.1.10"))
        XCTAssertTrue(S3Client.defaultPathStyle(forHost: ""))
    }

    // MARK: - Conditional create dialects

    func testStandardConditionalCreateUsesOnlyIfNoneMatchAndSignsIt() async throws {
        let recorder = S3RequestRecorder()
        S3MockURLProtocol.handler = { request in
            recorder.append(request)
            return .status(200)
        }
        let localURL = try makeTemporaryUploadFile("lock")
        defer { try? FileManager.default.removeItem(at: localURL) }

        try await makeClient(
            usePathStyle: false,
            sessionConfiguration: makeMockSessionConfiguration()
        ).upload(
            localURL: localURL,
            remotePath: "/locks/test.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        let request = try XCTUnwrap(recorder.requests.first)
        XCTAssertEqual(recorder.requests.count, 1)
        XCTAssertEqual(request.value(forHTTPHeaderField: "If-None-Match"), "*")
        XCTAssertNil(request.value(forHTTPHeaderField: "x-oss-forbid-overwrite"))
        let authorization = try XCTUnwrap(request.value(forHTTPHeaderField: "Authorization")).lowercased()
        XCTAssertTrue(authorization.contains("if-none-match"))
        XCTAssertFalse(authorization.contains("x-oss-forbid-overwrite"))
    }

    func testMultipartConditionalCreateAppliesConditionWhenPublishingObject() async throws {
        let recorder = S3RequestRecorder()
        S3MockURLProtocol.handler = { request in
            recorder.append(request)
            if request.httpMethod == "POST", request.url?.query?.contains("uploads") == true {
                return .data(Data("<InitiateMultipartUploadResult><UploadId>delivery-upload</UploadId></InitiateMultipartUploadResult>".utf8))
            }
            if request.httpMethod == "PUT", request.url?.query?.contains("partNumber=") == true {
                return .status(200, headers: ["ETag": "\"part\""])
            }
            if request.httpMethod == "POST", request.url?.query?.contains("uploadId=delivery-upload") == true {
                return .status(200)
            }
            return .status(400)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(repeating: 0x57, count: Int(S3Client.multipartThreshold + 1)).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        try await makeClient(
            usePathStyle: false,
            sessionConfiguration: makeMockSessionConfiguration()
        ).upload(
            localURL: localURL,
            remotePath: "/Inbox/large.mov",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        let completion = try XCTUnwrap(recorder.requests.first {
            $0.httpMethod == "POST" && $0.url?.query?.contains("uploadId=delivery-upload") == true
        })
        XCTAssertEqual(completion.value(forHTTPHeaderField: "If-None-Match"), "*")
        XCTAssertNil(completion.value(forHTTPHeaderField: "x-oss-forbid-overwrite"))
        let authorization = try XCTUnwrap(completion.value(forHTTPHeaderField: "Authorization")).lowercased()
        XCTAssertTrue(authorization.contains("if-none-match"))
    }

    func testMultipartConditionalPublishCollisionIsReportedAsNameCollision() async throws {
        let recorder = S3RequestRecorder()
        S3MockURLProtocol.handler = { request in
            recorder.append(request)
            if request.httpMethod == "POST", request.url?.query?.contains("uploads") == true {
                return .data(Data("<InitiateMultipartUploadResult><UploadId>delivery-collision</UploadId></InitiateMultipartUploadResult>".utf8))
            }
            if request.httpMethod == "PUT", request.url?.query?.contains("partNumber=") == true {
                return .status(200, headers: ["ETag": "\"part\""])
            }
            if request.httpMethod == "POST", request.url?.query?.contains("uploadId=delivery-collision") == true {
                return .xmlError(code: "PreconditionFailed", status: 412)
            }
            if request.httpMethod == "DELETE" { return .status(204) }
            return .status(400)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(repeating: 0x57, count: Int(S3Client.multipartThreshold + 1)).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await makeClient(
                usePathStyle: false,
                sessionConfiguration: makeMockSessionConfiguration()
            ).upload(
                localURL: localURL,
                remotePath: "/Inbox/large.mov",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected a name collision")
        } catch {
            XCTAssertTrue(remoteStorageIsNameCollision(error))
        }
        XCTAssertFalse(recorder.requests.contains { $0.httpMethod == "DELETE" })
    }

    func testMultipartReplaceFailureWaitsForAbortCleanup() async throws {
        let abortStarted = expectation(description: "abort started")
        S3MockURLProtocol.handler = { request in
            if request.httpMethod == "POST", request.url?.query?.contains("uploads") == true {
                return .data(Data("<InitiateMultipartUploadResult><UploadId>backup-failure</UploadId></InitiateMultipartUploadResult>".utf8))
            }
            if request.httpMethod == "PUT", request.url?.query?.contains("partNumber=") == true {
                return .status(500)
            }
            if request.httpMethod == "DELETE" {
                abortStarted.fulfill()
                Thread.sleep(forTimeInterval: 0.2)
                return .status(204)
            }
            return .status(400)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(repeating: 0x57, count: Int(S3Client.multipartThreshold + 1)).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        let start = Date()
        do {
            try await makeClient(
                usePathStyle: false,
                sessionConfiguration: makeMockSessionConfiguration()
            ).upload(
                localURL: localURL,
                remotePath: "/backup/large.mov",
                mode: .replace,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected multipart upload failure")
        } catch {
            XCTAssertGreaterThanOrEqual(Date().timeIntervalSince(start), 0.15)
        }
        await fulfillment(of: [abortStarted], timeout: 1)
    }

    func testMultipartReplaceCancellationStillCompletesAbortCleanup() async throws {
        let recorder = S3RequestRecorder()
        S3MockURLProtocol.handler = { request in
            recorder.append(request)
            if request.httpMethod == "POST", request.url?.query?.contains("uploads") == true {
                return .data(Data("<InitiateMultipartUploadResult><UploadId>cancelled-backup</UploadId></InitiateMultipartUploadResult>".utf8))
            }
            if request.httpMethod == "PUT", request.url?.query?.contains("partNumber=") == true {
                return .status(200, headers: ["ETag": "\"part\""])
            }
            if request.httpMethod == "DELETE" { return .status(204) }
            if request.httpMethod == "POST", request.url?.query?.contains("uploadId=cancelled-backup") == true {
                return .status(200)
            }
            return .status(400)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(repeating: 0x57, count: Int(S3Client.multipartThreshold + 1)).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        let cancellation = S3TestCancellationHandle()
        let task = Task {
            try await makeClient(
                usePathStyle: false,
                sessionConfiguration: makeMockSessionConfiguration()
            ).upload(
                localURL: localURL,
                remotePath: "/backup/large.mov",
                mode: .replace,
                respectTaskCancellation: true,
                onProgress: { progress in
                    if progress > 0 { cancellation.cancel() }
                }
            )
        }
        cancellation.install { task.cancel() }

        do {
            try await task.value
            XCTFail("Expected cancellation")
        } catch {
            XCTAssertTrue(task.isCancelled)
        }
        XCTAssertTrue(recorder.requests.contains { $0.httpMethod == "DELETE" })
    }

    func testAliyunConditionalCreateUsesOnlyOSSHeaderWithoutRuntimeProbe() async throws {
        let recorder = S3RequestRecorder()
        S3MockURLProtocol.handler = { request in
            recorder.append(request)
            return .status(200)
        }
        let localURL = try makeTemporaryUploadFile("lock")
        defer { try? FileManager.default.removeItem(at: localURL) }

        try await makeClient(
            usePathStyle: false,
            host: "s3.oss-cn-hangzhou.aliyuncs.com",
            region: "cn-hangzhou",
            sessionConfiguration: makeMockSessionConfiguration()
        ).upload(
            localURL: localURL,
            remotePath: "/locks/test.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        let request = try XCTUnwrap(recorder.requests.first)
        XCTAssertEqual(recorder.requests.count, 1)
        XCTAssertNil(request.value(forHTTPHeaderField: "If-None-Match"))
        XCTAssertEqual(request.value(forHTTPHeaderField: "x-oss-forbid-overwrite"), "true")
        XCTAssertEqual(request.url?.host, "examplebucket.s3.oss-cn-hangzhou.aliyuncs.com")
        let authorization = try XCTUnwrap(request.value(forHTTPHeaderField: "Authorization")).lowercased()
        XCTAssertTrue(authorization.contains("x-oss-forbid-overwrite"))
        XCTAssertTrue(authorization.contains("/cn-hangzhou/s3/aws4_request"))
    }

    func testAliyunCNAMEConditionalCreateUsesOSSHeaderAndSignsCustomHost() async throws {
        let recorder = S3RequestRecorder()
        S3MockURLProtocol.handler = { request in
            recorder.append(request)
            return .status(200)
        }
        let localURL = try makeTemporaryUploadFile("lock")
        defer { try? FileManager.default.removeItem(at: localURL) }

        try await makeClient(
            usePathStyle: true,
            host: "backup.example.com",
            region: "cn-hangzhou",
            provider: .aliyunOSS,
            sessionConfiguration: makeMockSessionConfiguration()
        ).upload(
            localURL: localURL,
            remotePath: "/locks/test.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        let request = try XCTUnwrap(recorder.requests.first)
        XCTAssertEqual(request.url?.host, "backup.example.com")
        XCTAssertEqual(request.url?.path, "/locks/test.lock")
        XCTAssertNil(request.value(forHTTPHeaderField: "If-None-Match"))
        XCTAssertEqual(request.value(forHTTPHeaderField: "x-oss-forbid-overwrite"), "true")
        let authorization = try XCTUnwrap(request.value(forHTTPHeaderField: "Authorization")).lowercased()
        XCTAssertTrue(authorization.contains("host;x-amz-content-sha256;x-amz-date;x-oss-forbid-overwrite"))
        XCTAssertTrue(authorization.contains("/cn-hangzhou/s3/aws4_request"))
    }

    func testAliyunCNAMEConnectRejectsDifferentBoundBucket() async throws {
        S3MockURLProtocol.handler = { request in
            guard request.httpMethod == "GET",
                  request.url?.query?.contains("list-type=2") == true else {
                return .status(400)
            }
            return .data(Data("<ListBucketResult><Name>anotherbucket</Name></ListBucketResult>".utf8))
        }
        let client = makeClient(
            usePathStyle: true,
            host: "backup.example.com",
            region: "cn-hangzhou",
            provider: .aliyunOSS,
            sessionConfiguration: makeMockSessionConfiguration()
        )

        do {
            try await client.connect()
            XCTFail("Expected the bound Bucket mismatch to be rejected")
        } catch RemoteStorageClientError.invalidConfiguration {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testAliyunProfileVerifierAcceptsNonVersionedCollision() async throws {
        let server = S3ProbeMockServer(secondConditionalWrite: .collision)
        S3MockURLProtocol.handler = server.response
        let client = makeClient(
            usePathStyle: false,
            host: "oss-cn-hangzhou.aliyuncs.com",
            region: "cn-hangzhou",
            sessionConfiguration: makeMockSessionConfiguration()
        )

        try await S3ProfileVerifier.run(
            client: client,
            writeAccessMessageTemplate: "Write probe failed: %@",
            timeout: 2
        )

        XCTAssertEqual(server.lastWrittenSourceData, Data("watermelon-write-probe-a".utf8))
        XCTAssertTrue(server.remainingObjectPaths.isEmpty)
        try assertAliyunVerifierRequests(
            server.requests,
            expectedHost: "examplebucket.oss-cn-hangzhou.aliyuncs.com"
        )
    }

    func testAliyunCNAMEProfileVerifierAcceptsVersionedOverwrite() async throws {
        let server = S3ProbeMockServer(secondConditionalWrite: .overwrite)
        S3MockURLProtocol.handler = server.response
        let client = makeClient(
            usePathStyle: true,
            host: "backup.example.com",
            region: "cn-hangzhou",
            provider: .aliyunOSS,
            sessionConfiguration: makeMockSessionConfiguration()
        )

        try await S3ProfileVerifier.run(
            client: client,
            writeAccessMessageTemplate: "Write probe failed: %@",
            timeout: 2
        )

        XCTAssertEqual(server.lastWrittenSourceData, Data("watermelon-write-probe-b".utf8))
        XCTAssertTrue(server.remainingObjectPaths.isEmpty)
        try assertAliyunVerifierRequests(
            server.requests,
            expectedHost: "backup.example.com"
        )
    }

    func testAliyunOfficialEndpointProfileVerifierAcceptsVersionedOverwrite() async throws {
        let server = S3ProbeMockServer(secondConditionalWrite: .overwrite)
        S3MockURLProtocol.handler = server.response
        let client = makeClient(
            usePathStyle: false,
            host: "oss-cn-hangzhou.aliyuncs.com",
            region: "cn-hangzhou",
            sessionConfiguration: makeMockSessionConfiguration()
        )

        try await S3ProfileVerifier.run(
            client: client,
            writeAccessMessageTemplate: "Write probe failed: %@",
            timeout: 2
        )

        XCTAssertEqual(server.lastWrittenSourceData, Data("watermelon-write-probe-b".utf8))
        XCTAssertTrue(server.remainingObjectPaths.isEmpty)
        try assertAliyunVerifierRequests(
            server.requests,
            expectedHost: "examplebucket.oss-cn-hangzhou.aliyuncs.com"
        )
    }

    func testStandardVerifierRejectsSuccessfulSecondConditionalWrite() async throws {
        let server = S3ProbeMockServer(secondConditionalWrite: .overwrite)
        S3MockURLProtocol.handler = server.response
        let client = makeClient(
            usePathStyle: false,
            sessionConfiguration: makeMockSessionConfiguration(),
            verificationCleanupRetryDelays: [0]
        )

        do {
            try await client.verifyWriteAccess()
            XCTFail("Expected unsafe conditional create rejection")
        } catch let error as RemoteStorageClientError {
            guard case .unsafeConditionalCreateUnsupported = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }

        for _ in 0..<100 where server.deleteCount < 2 {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTAssertEqual(server.deleteCount, 2)
        XCTAssertTrue(server.remainingObjectPaths.isEmpty)
    }

    func testUnrelated409IsNotReportedAsNameCollision() async throws {
        S3MockURLProtocol.handler = { _ in
            .xmlError(code: "FileImmutable", status: 409)
        }
        let localURL = try makeTemporaryUploadFile("lock")
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await makeClient(
                usePathStyle: false,
                sessionConfiguration: makeMockSessionConfiguration()
            ).upload(
                localURL: localURL,
                remotePath: "/locks/test.lock",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected FileImmutable")
        } catch {
            XCTAssertFalse(remoteStorageIsNameCollision(error))
        }
    }

    func testFileAlreadyExistsIsReportedAsNameCollision() async throws {
        S3MockURLProtocol.handler = { _ in
            .xmlError(code: "FileAlreadyExists", status: 409)
        }
        let localURL = try makeTemporaryUploadFile("lock")
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await makeClient(
                usePathStyle: false,
                sessionConfiguration: makeMockSessionConfiguration()
            ).upload(
                localURL: localURL,
                remotePath: "/locks/test.lock",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected FileAlreadyExists")
        } catch {
            XCTAssertTrue(remoteStorageIsNameCollision(error))
        }
    }

    func testBare409RemainsNameCollisionForCompatibleBackends() async throws {
        S3MockURLProtocol.handler = { _ in .status(409) }
        let localURL = try makeTemporaryUploadFile("lock")
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await makeClient(
                usePathStyle: false,
                sessionConfiguration: makeMockSessionConfiguration()
            ).upload(
                localURL: localURL,
                remotePath: "/locks/test.lock",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected a name collision")
        } catch {
            XCTAssertTrue(remoteStorageIsNameCollision(error))
        }
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

    func testMultipartThresholdConstantsMatchAWSBounds() {
        XCTAssertGreaterThanOrEqual(S3Client.multipartPartSize, 5 * 1024 * 1024)
        XCTAssertEqual(S3Client.multipartThreshold, S3Client.multipartPartSize)
    }

    func testPartSizeUsesBaselineForFilesUnderMaxAtBaseline() {
        let oneMiB: Int64 = 1024 * 1024
        XCTAssertEqual(S3Client.partSize(forFileSize: 0), 8 * oneMiB)
        XCTAssertEqual(S3Client.partSize(forFileSize: 100 * oneMiB), 8 * oneMiB)
        XCTAssertEqual(S3Client.partSize(forFileSize: 50 * 1024 * oneMiB), 8 * oneMiB)
    }

    func testPartSizeGrowsForFilesAboveTargetCount() {
        let oneMiB: Int64 = 1024 * 1024
        XCTAssertEqual(S3Client.partSize(forFileSize: 100 * 1024 * oneMiB), 16 * oneMiB)
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

    // MARK: - Default region per provider

    func testDefaultRegionForR2IsAuto() {
        XCTAssertEqual(S3Client.defaultRegion(forHost: "abc123.r2.cloudflarestorage.com"), "auto")
        XCTAssertEqual(S3Client.defaultRegion(forHost: "ABC123.R2.CLOUDFLARESTORAGE.COM"), "auto")
        XCTAssertEqual(S3Client.defaultRegion(forHost: "abc123.r2.cloudflarestorage.com."), "auto")
        XCTAssertEqual(
            S3Client.effectiveSigningRegion(userInput: "", host: "abc123.r2.cloudflarestorage.com."),
            "auto"
        )
    }

    func testDefaultRegionForAWSExtractsRegionFromHost() {
        XCTAssertEqual(S3Client.defaultRegion(forHost: "s3.us-east-1.amazonaws.com"), "us-east-1")
        XCTAssertEqual(S3Client.defaultRegion(forHost: "s3.eu-west-2.amazonaws.com"), "eu-west-2")
        XCTAssertEqual(S3Client.defaultRegion(forHost: "s3.ap-northeast-1.amazonaws.com"), "ap-northeast-1")
        XCTAssertEqual(S3Client.defaultRegion(forHost: "s3.eu-west-2.amazonaws.com."), "eu-west-2")
        XCTAssertEqual(
            S3Client.effectiveSigningRegion(userInput: "", host: "s3.eu-west-2.amazonaws.com."),
            S3Client.effectiveSigningRegion(userInput: "", host: "s3.eu-west-2.amazonaws.com")
        )
    }

    func testDefaultRegionForB2ExtractsRegionFromHost() {
        XCTAssertEqual(S3Client.defaultRegion(forHost: "s3.us-west-002.backblazeb2.com"), "us-west-002")
        XCTAssertEqual(S3Client.defaultRegion(forHost: "s3.eu-central-003.backblazeb2.com"), "eu-central-003")
    }

    func testDefaultRegionForWasabiExtractsRegionFromHost() {
        XCTAssertEqual(S3Client.defaultRegion(forHost: "s3.us-east-1.wasabisys.com"), "us-east-1")
        XCTAssertEqual(S3Client.defaultRegion(forHost: "s3.ap-northeast-1.wasabisys.com"), "ap-northeast-1")
    }

    func testDefaultRegionForDigitalOceanExtractsRegionFromHost() {
        XCTAssertEqual(S3Client.defaultRegion(forHost: "nyc3.digitaloceanspaces.com"), "nyc3")
        XCTAssertEqual(S3Client.defaultRegion(forHost: "fra1.digitaloceanspaces.com"), "fra1")
    }

    func testDefaultRegionForUnknownHostReturnsNil() {
        XCTAssertNil(S3Client.defaultRegion(forHost: "minio.local"))
        XCTAssertNil(S3Client.defaultRegion(forHost: "192.168.1.10"))
        XCTAssertNil(S3Client.defaultRegion(forHost: ""))
        XCTAssertNil(S3Client.defaultRegion(forHost: "amazonaws.com"))
    }

    func testDefaultRegionRejectsBareSuffix() {
        XCTAssertNil(S3Client.defaultRegion(forHost: ".amazonaws.com"))
        XCTAssertNil(S3Client.defaultRegion(forHost: "s3..amazonaws.com"))
        XCTAssertNil(S3Client.defaultRegion(forHost: "s3.us-east-1.extra.amazonaws.com"))
    }

    // MARK: - Path-style auto-detection (provider expansion)

    func testDefaultPathStyleForDigitalOceanAndWasabiIsVirtualHosted() {
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "nyc3.digitaloceanspaces.com"))
        XCTAssertFalse(S3Client.defaultPathStyle(forHost: "s3.us-east-1.wasabisys.com"))
    }
}

private final class S3RequestRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [URLRequest] = []

    var requests: [URLRequest] { lock.withLock { storage } }

    func append(_ request: URLRequest) {
        lock.withLock { storage.append(request) }
    }
}

private final class S3TestCancellationHandle: @unchecked Sendable {
    private let lock = NSLock()
    private var operation: (() -> Void)?
    private var cancellationRequested = false

    func install(_ operation: @escaping () -> Void) {
        let shouldCancel = lock.withLock {
            if cancellationRequested { return true }
            self.operation = operation
            return false
        }
        if shouldCancel { operation() }
    }

    func cancel() {
        let operation = lock.withLock {
            cancellationRequested = true
            let operation = self.operation
            self.operation = nil
            return operation
        }
        operation?()
    }
}

private final class S3ProbeMockServer: @unchecked Sendable {
    enum SecondConditionalWrite {
        case collision
        case overwrite
    }

    private let lock = NSLock()
    private let recorder = S3RequestRecorder()
    private let secondConditionalWrite: SecondConditionalWrite
    private var conditionalWriteCount = 0
    private var sourcePath: String?
    private var destinationPath: String?
    private var sourceData: Data?
    private var destinationData: Data?
    private var lastSourceData: Data?

    init(secondConditionalWrite: SecondConditionalWrite) {
        self.secondConditionalWrite = secondConditionalWrite
    }

    var requests: [URLRequest] { recorder.requests }
    var lastWrittenSourceData: Data? { lock.withLock { lastSourceData } }
    var remainingObjectPaths: Set<String> {
        lock.withLock {
            var paths: Set<String> = []
            if sourceData != nil, let sourcePath { paths.insert(sourcePath) }
            if destinationData != nil, let destinationPath { paths.insert(destinationPath) }
            return paths
        }
    }
    var deleteCount: Int {
        requests.filter { $0.httpMethod == "DELETE" }.count
    }

    func response(for request: URLRequest) -> S3MockURLProtocol.Response {
        recorder.append(request)
        guard let url = request.url else { return .status(400) }

        if request.httpMethod == "GET", url.query?.contains("list-type=2") == true {
            return .data(Data("<ListBucketResult><Name>examplebucket</Name></ListBucketResult>".utf8))
        }
        let isConditionalWrite = request.value(forHTTPHeaderField: "x-oss-forbid-overwrite") == "true"
            || request.value(forHTTPHeaderField: "If-None-Match") == "*"
        if request.httpMethod == "PUT", isConditionalWrite {
            return lock.withLock {
                conditionalWriteCount += 1
                if conditionalWriteCount == 1 {
                    sourcePath = url.path
                    sourceData = Data("watermelon-write-probe-a".utf8)
                    lastSourceData = sourceData
                    return .status(200)
                }
                switch secondConditionalWrite {
                case .collision:
                    return .xmlError(code: "FileAlreadyExists", status: 409)
                case .overwrite:
                    sourceData = Data("watermelon-write-probe-b".utf8)
                    lastSourceData = sourceData
                    return .status(200)
                }
            }
        }
        if request.httpMethod == "PUT",
           request.value(forHTTPHeaderField: "x-amz-copy-source") != nil {
            lock.withLock {
                destinationPath = url.path
                destinationData = sourceData
            }
            return .data(Data("<CopyObjectResult><ETag>\"probe\"</ETag></CopyObjectResult>".utf8))
        }
        if request.httpMethod == "GET" {
            let data = lock.withLock { () -> Data? in
                if url.path == sourcePath { return sourceData }
                if url.path == destinationPath { return destinationData }
                return nil
            }
            return data.map { .data($0) } ?? .xmlError(code: "NoSuchKey", status: 404)
        }
        if request.httpMethod == "DELETE" {
            lock.withLock {
                if url.path == sourcePath { sourceData = nil }
                if url.path == destinationPath { destinationData = nil }
            }
            return .status(204)
        }
        return .status(400)
    }
}

private final class S3MockURLProtocol: URLProtocol {
    struct Response {
        let body: Data
        let status: Int
        let headers: [String: String]

        static func status(_ status: Int, headers: [String: String] = [:]) -> Response {
            Response(body: Data(), status: status, headers: headers)
        }

        static func data(_ data: Data, status: Int = 200) -> Response {
            Response(body: data, status: status, headers: [:])
        }

        static func xmlError(
            code: String,
            status: Int,
            headers: [String: String] = [:]
        ) -> Response {
            return Response(
                body: Data("<Error><Code>\(code)</Code></Error>".utf8),
                status: status,
                headers: headers.merging(["Content-Type": "application/xml"]) { current, _ in current }
            )
        }
    }

    private static let handlerLock = NSLock()
    private static var storedHandler: ((URLRequest) throws -> Response)?
    static var handler: ((URLRequest) throws -> Response)? {
        get { handlerLock.withLock { storedHandler } }
        set { handlerLock.withLock { storedHandler = newValue } }
    }
    override class func canInit(with _: URLRequest) -> Bool { true }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler,
              let url = request.url else {
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
            return
        }
        do {
            let result = try handler(request)
            guard let response = HTTPURLResponse(
                url: url,
                statusCode: result.status,
                httpVersion: "HTTP/1.1",
                headerFields: result.headers
            ) else {
                throw URLError(.badServerResponse)
            }
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            if !result.body.isEmpty {
                client?.urlProtocol(self, didLoad: result.body)
            }
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}
