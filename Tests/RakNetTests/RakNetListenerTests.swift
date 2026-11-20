@testable import RakNet
import Testing

@Test func listenerCloseEndsAccept() async throws {
    let address = IPv4Endpoint(address: 0x7f000001, port: 0)
    let listener = try RakNetListener.listen(on: address)
    #expect(listener.localAddress.port != 0)

    let waiting = Task { try await listener.accept() }
    listener.close()
    let connection = try await waiting.value
    #expect(connection == nil)
}

@Test func listenerCancellationDoesNotCloseListener() async throws {
    let address = IPv4Endpoint(address: 0x7f000001, port: 0)
    let listener = try RakNetListener.listen(on: address)
    defer { listener.close() }

    let abandoned = Task { try await listener.accept() }
    abandoned.cancel()
    await #expect(throws: CancellationError.self) {
        try await abandoned.value
    }

    let waiting = Task { try await listener.accept() }
    listener.close()
    let connection = try await waiting.value
    #expect(connection == nil)
}

@Test func listenerBlocksDiscoveryUntilUnblocked() async throws {
    let endpoint = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 0))
    let listener = try RakNetListener.listen(on: endpoint, advertisement: [42])
    defer { listener.close() }

    listener.block(endpoint)
    await #expect(throws: RakNetTransportError.timedOut) {
        try await RakNetConnection.ping(listener.localAddress, timeout: .milliseconds(100))
    }
    listener.unblock(listener.localAddress)
    let pong = try await RakNetConnection.ping(listener.localAddress)
    #expect(pong.advertisement == [42])
}

@Test func temporaryBlocksExpireAndIgnoreThePort() {
    let first = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 1, port: 2000))
    let second = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 1, port: 2001))
    let other = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0x2001_0db8_0000_0001, addressLow: 2, port: 2000))
    let now = ContinuousClock.now
    let access = ListenerAccess()
    access.block(first, for: .seconds(1), at: now)
    access.block(second, for: .milliseconds(1), at: now)
    #expect(access.isBlocked(second, at: now.advanced(by: .milliseconds(999))))
    #expect(!access.isBlocked(other, at: now))
    #expect(!access.isBlocked(first, at: now.advanced(by: .seconds(1))))
}

@Test func offlineRateLimitsEachIPAndTheWholeListener() {
    let first = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 1000))
    let second = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 1001))
    let third = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000002, port: 1000))
    var rate = OfflinePacketRate(limits: .init(perIP: 1, total: 2))

    let fourth = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 1, port: 1000))
    let attempts: [(RakNetEndpoint, UInt64, Bool)] = [
        (first, 0, true), (second, 1, false), (third, 1, true), (fourth, 999, false),
        (second, 1_000, true), (third, 1_000, true)
    ]
    for (endpoint, now, expected) in attempts {
        let allowed = rate.allow(endpoint, at: now)
        #expect(allowed == expected)
    }
}

@Test(.timeLimit(.minutes(1))) func listenerReleasesPerIPConnectionSlots() async throws {
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: 0)
    let listener = try RakNetListener.listen(on: endpoint, maximumConnectionsPerIP: 1)
    defer { listener.close() }

    let first = try await RakNetConnection.dial(listener.localAddress)
    let incoming = try await listener.accept()
    let server = try #require(incoming)
    await #expect(throws: RakNetTransportError.timedOut) {
        try await RakNetConnection.dial(listener.localAddress, timeout: .milliseconds(200))
    }

    try await first.close()
    let ended = try await server.receive()
    #expect(ended == nil)
    let next = try await RakNetConnection.dial(listener.localAddress)
    try await next.close()
}

@Test func onlineRateRefillsAndTemporarilyBlocksOverruns() {
    var rate = OnlinePacketRate(packetsPerSecond: 2, at: 0)
    let attempts: [(UInt64, Bool)] = [
        (0, true), (0, true), (500, true), (501, false),
        (10_500, false), (10_501, true), (10_501, true), (10_501, false)
    ]
    for (now, expected) in attempts {
        let allowed = rate.allow(at: now)
        #expect(allowed == expected)
    }
}
