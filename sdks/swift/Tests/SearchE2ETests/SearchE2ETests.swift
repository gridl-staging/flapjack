import XCTest
import Core
import Search

final class SearchE2ETests: XCTestCase {
    static let testIndex = "test_swift_sdk"
    static var client: SearchClient!

    override class func setUp() {
        super.setUp()

        let appID = ProcessInfo.processInfo.environment["FLAPJACK_APP_ID"] ?? "test-app"
        let apiKey = ProcessInfo.processInfo.environment["FLAPJACK_API_KEY"] ?? "test-api-key"
        let host = ProcessInfo.processInfo.environment["FLAPJACK_HOST"] ?? "localhost"
        let port = Int(ProcessInfo.processInfo.environment["FLAPJACK_PORT"] ?? "7700") ?? 7700

        let configuration = try! SearchClientConfiguration(
            appID: appID,
            apiKey: apiKey,
            hosts: [Host(url: host, port: port, scheme: "http", callType: .readWrite)]
        )
        client = SearchClient(configuration: configuration)

        // Seed test data
        let settings = IndexSettings(
            searchableAttributes: ["name", "brand", "category"],
            attributesForFaceting: ["brand", "category", "price"]
        )
        _ = try? client.setSettings(indexName: testIndex, indexSettings: settings)

        let objects: [BatchRequest] = [
            BatchRequest(action: .addObject, body: ["objectID": "phone1", "name": "iPhone 15 Pro", "brand": "Apple", "category": "Phone", "price": 999]),
            BatchRequest(action: .addObject, body: ["objectID": "phone2", "name": "Samsung Galaxy S24", "brand": "Samsung", "category": "Phone", "price": 799]),
            BatchRequest(action: .addObject, body: ["objectID": "laptop1", "name": "MacBook Pro M3", "brand": "Apple", "category": "Laptop", "price": 1999]),
            BatchRequest(action: .addObject, body: ["objectID": "laptop2", "name": "Google Pixel 8", "brand": "Google", "category": "Phone", "price": 699]),
            BatchRequest(action: .addObject, body: ["objectID": "laptop3", "name": "Dell XPS 15", "brand": "Dell", "category": "Laptop", "price": 1299]),
        ]
        _ = try? client.batch(indexName: testIndex, batchWriteParams: BatchWriteParams(requests: objects))

        Thread.sleep(forTimeInterval: 0.5)
    }

    // MARK: - List Indices

    func testListIndices() async throws {
        let response = try await Self.client.listIndices()
        XCTAssertNotNil(response.items)
        let found = response.items?.contains(where: { $0.name == Self.testIndex }) ?? false
        XCTAssertTrue(found, "Test index should appear in listIndices")
    }

    // MARK: - Basic Search

    func testBasicSearch() async throws {
        let response = try await Self.client.search(
            searchMethodParams: SearchMethodParams(requests: [
                SearchQuery.searchForHits(SearchForHits(query: "iPhone", indexName: Self.testIndex))
            ])
        ) as SearchResponses<[String: AnyCodable]>
        XCTAssertFalse(response.results.isEmpty)
    }

    // MARK: - Empty Query Returns All

    func testEmptyQueryReturnsAll() async throws {
        let response = try await Self.client.search(
            searchMethodParams: SearchMethodParams(requests: [
                SearchQuery.searchForHits(SearchForHits(query: "", indexName: Self.testIndex))
            ])
        ) as SearchResponses<[String: AnyCodable]>
        XCTAssertFalse(response.results.isEmpty)
    }

    // MARK: - Search With Filters

    func testSearchWithFilters() async throws {
        let response = try await Self.client.search(
            searchMethodParams: SearchMethodParams(requests: [
                SearchQuery.searchForHits(SearchForHits(query: "", filters: "brand:Apple", indexName: Self.testIndex))
            ])
        ) as SearchResponses<[String: AnyCodable]>
        XCTAssertFalse(response.results.isEmpty)
    }

    // MARK: - Search With Facets

    func testSearchWithFacets() async throws {
        let response = try await Self.client.search(
            searchMethodParams: SearchMethodParams(requests: [
                SearchQuery.searchForHits(SearchForHits(query: "", facets: ["brand", "category"], indexName: Self.testIndex))
            ])
        ) as SearchResponses<[String: AnyCodable]>
        XCTAssertFalse(response.results.isEmpty)
    }

    // MARK: - Get Object

    func testGetObject() async throws {
        let obj = try await Self.client.getObject(indexName: Self.testIndex, objectID: "phone1") as [String: AnyCodable]
        XCTAssertEqual(obj["name"]?.value as? String, "iPhone 15 Pro")
        XCTAssertEqual(obj["brand"]?.value as? String, "Apple")
    }

    // MARK: - Get Settings

    func testGetSettings() async throws {
        let settings = try await Self.client.getSettings(indexName: Self.testIndex)
        XCTAssertNotNil(settings.searchableAttributes)
        XCTAssertTrue(settings.searchableAttributes?.contains("name") ?? false)
    }

    // MARK: - Save and Search Synonyms

    func testSaveAndSearchSynonyms() async throws {
        let synonym = SynonymHit(objectID: "syn-phone-mobile", type: .synonym, synonyms: ["phone", "mobile", "smartphone"])
        _ = try await Self.client.saveSynonyms(indexName: Self.testIndex, synonymHit: [synonym])
        try await Task.sleep(nanoseconds: 500_000_000)

        let response = try await Self.client.searchSynonyms(indexName: Self.testIndex)
        XCTAssertTrue(response.nbHits >= 1)
    }

    // MARK: - Browse Cursor Pagination

    func testBrowseCursorPagination() async throws {
        let response = try await Self.client.browse(
            indexName: Self.testIndex,
            browseParams: BrowseParamsObject(hitsPerPage: 2)
        )
        XCTAssertNotNil(response.hits)
        XCTAssertEqual(response.hits?.count, 2, "Should return 2 hits per page")
        XCTAssertNotNil(response.cursor, "Should have cursor for next page")

        // Follow cursor
        let page2 = try await Self.client.browse(
            indexName: Self.testIndex,
            browseParams: BrowseParamsObject(cursor: response.cursor, hitsPerPage: 2)
        )
        XCTAssertNotNil(page2.hits)
        XCTAssertTrue((page2.hits?.count ?? 0) >= 1, "Second page should have at least 1 hit")
    }

    // MARK: - Stage 1 Settings Round-Trip

    func testSettingsNumericAttributesForFilteringRoundtrip() async throws {
        let settings = IndexSettings(numericAttributesForFiltering: ["price", "rating"])
        _ = try await Self.client.setSettings(indexName: Self.testIndex, indexSettings: settings)
        try await Task.sleep(nanoseconds: 500_000_000)

        let result = try await Self.client.getSettings(indexName: Self.testIndex)
        XCTAssertNotNil(result.numericAttributesForFiltering)
        XCTAssertTrue(result.numericAttributesForFiltering?.contains("price") ?? false)
        XCTAssertTrue(result.numericAttributesForFiltering?.contains("rating") ?? false)
    }

    func testSettingsUnorderedSearchableAttributesRoundtrip() async throws {
        let settings = IndexSettings(searchableAttributes: ["unordered(name)", "brand", "unordered(description)"])
        _ = try await Self.client.setSettings(indexName: Self.testIndex, indexSettings: settings)
        try await Task.sleep(nanoseconds: 500_000_000)

        let result = try await Self.client.getSettings(indexName: Self.testIndex)
        XCTAssertTrue(result.searchableAttributes?.contains("unordered(name)") ?? false, "Should contain unordered(name)")
        XCTAssertTrue(result.searchableAttributes?.contains("unordered(description)") ?? false, "Should contain unordered(description)")

        // Restore
        _ = try await Self.client.setSettings(indexName: Self.testIndex, indexSettings: IndexSettings(
            searchableAttributes: ["name", "brand", "category"]
        ))
    }

    func testSettingsAllowCompressionOfIntegerArrayRoundtrip() async throws {
        let settings = IndexSettings(allowCompressionOfIntegerArray: true)
        _ = try await Self.client.setSettings(indexName: Self.testIndex, indexSettings: settings)
        try await Task.sleep(nanoseconds: 500_000_000)

        let result = try await Self.client.getSettings(indexName: Self.testIndex)
        XCTAssertEqual(result.allowCompressionOfIntegerArray, true)
    }

    // MARK: - API Key CRUD

    func testApiKeyCrud() async throws {
        let host = ProcessInfo.processInfo.environment["FLAPJACK_HOST"] ?? "localhost"
        let port = Int(ProcessInfo.processInfo.environment["FLAPJACK_PORT"] ?? "7700") ?? 7700
        let baseURL = "http://\(host):\(port)"
        let apiKey = ProcessInfo.processInfo.environment["FLAPJACK_API_KEY"] ?? "test-api-key"
        let appId = ProcessInfo.processInfo.environment["FLAPJACK_APP_ID"] ?? "test-app"

        // Create key
        var createReq = URLRequest(url: URL(string: "\(baseURL)/1/keys")!)
        createReq.httpMethod = "POST"
        createReq.setValue("application/json", forHTTPHeaderField: "Content-Type")
        createReq.setValue(apiKey, forHTTPHeaderField: "x-algolia-api-key")
        createReq.setValue(appId, forHTTPHeaderField: "x-algolia-application-id")
        createReq.httpBody = try JSONSerialization.data(withJSONObject: [
            "acl": ["search", "browse"],
            "description": "Swift SDK matrix test key",
            "indexes": [Self.testIndex]
        ])

        let (createData, createResp) = try await URLSession.shared.data(for: createReq)
        let createHTTP = createResp as! HTTPURLResponse
        XCTAssertEqual(createHTTP.statusCode, 200, "Create key should return 200")

        let createResult = try JSONSerialization.jsonObject(with: createData) as! [String: Any]
        let key = createResult["key"] as! String
        XCTAssertFalse(key.isEmpty)

        // List keys
        var listReq = URLRequest(url: URL(string: "\(baseURL)/1/keys")!)
        listReq.setValue(apiKey, forHTTPHeaderField: "x-algolia-api-key")
        listReq.setValue(appId, forHTTPHeaderField: "x-algolia-application-id")

        let (listData, _) = try await URLSession.shared.data(for: listReq)
        let listResult = try JSONSerialization.jsonObject(with: listData) as! [String: Any]
        XCTAssertNotNil(listResult["keys"], "Should have keys array")

        // Delete key
        var deleteReq = URLRequest(url: URL(string: "\(baseURL)/1/keys/\(key)")!)
        deleteReq.httpMethod = "DELETE"
        deleteReq.setValue(apiKey, forHTTPHeaderField: "x-algolia-api-key")
        deleteReq.setValue(appId, forHTTPHeaderField: "x-algolia-application-id")

        let (_, deleteResp) = try await URLSession.shared.data(for: deleteReq)
        let deleteHTTP = deleteResp as! HTTPURLResponse
        XCTAssertEqual(deleteHTTP.statusCode, 200, "Delete key should return 200")
    }

    // MARK: - User Agent Contains Flapjack

    func testUserAgentContainsFlapjack() {
        let agent = UserAgent.library
        XCTAssertTrue(agent.title.contains("Flapjack"), "User-Agent should contain 'Flapjack', got: \(agent.title)")
    }
}
