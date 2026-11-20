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
    let other = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 2, port: 2000))
    let now = ContinuousClock.now
    let access = ListenerAccess()
    access.block(first, for: .seconds(1), at: now)
    access.block(second, for: .milliseconds(1), at: now)
    #expect(access.isBlocked(second, at: now.advanced(by: .milliseconds(999))))
    #expect(!access.isBlocked(other, at: now))
    #expect(!access.isBlocked(first, at: now.advanced(by: .seconds(1))))
}
