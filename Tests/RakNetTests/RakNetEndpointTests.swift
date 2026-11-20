import RakNet
import Testing

@Test func resolvesNumericEndpoints() async throws {
    let ipv4 = try await RakNetEndpoint.resolve("127.0.0.1", port: 19132)
    let ipv6 = try await RakNetEndpoint.resolve("::1", port: 19133)
    #expect(ipv4 == [.ipv4(IPv4Endpoint(address: 0x7f000001, port: 19132))])
    #expect(ipv6 == [.ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 1, port: 19133))])
}

@Test func resolvesLocalhostAndRejectsTruncatedHostnames() async throws {
    let endpoints = try await RakNetEndpoint.resolve("localhost", port: 19132)
    #expect(!endpoints.isEmpty)
    #expect(endpoints.allSatisfy { $0.port == 19132 })
    #expect(Set(endpoints).count == endpoints.count)
    await #expect(throws: RakNetEndpoint.ResolutionError.invalidHost) {
        try await RakNetEndpoint.resolve("localhost\0invalid", port: 19132)
    }
}
