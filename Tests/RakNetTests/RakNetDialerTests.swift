@testable import RakNet
import Testing

@Test(.timeLimit(.minutes(1)), arguments: [false, true])
func dialExchangesPayloadAndCloses(useIPv6: Bool) async throws {
    let address: RakNetEndpoint = useIPv6
        ? .ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 1, port: 0))
        : .ipv4(IPv4Endpoint(address: 0x7f000001, port: 0))
    let listener = try RakNetListener.listen(on: address)
    defer { listener.close() }

    let remote: RakNetEndpoint
    if case let .ipv6(endpoint) = listener.localAddress {
        remote = .ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 1, port: endpoint.port, flowInfo: 1))
    } else {
        remote = listener.localAddress
    }
    let client = try await RakNetConnection.dial(remote)
    let incoming = try await expectWithin("accept") { try await listener.accept() }
    let server = try #require(incoming)
    #expect(client.remoteAddress == listener.localAddress)
    #expect(server.remoteAddress == client.localAddress)
    #expect(client.mtu == IPv4MTU.defaultValue)

    let payload = [UInt8](repeating: 0xfe, count: 4096)
    try await client.send(payload)
    let request = try await expectWithin("request") { try await server.receive() }
    #expect(request == payload)
    try await server.send([0xfe, 0x42], reliability: .reliableSequenced, channel: 3)
    let reply = try await expectWithin("reply") { try await client.receive() }
    #expect(reply == [0xfe, 0x42])
    let latency = try await expectWithin("round trip time") {
        while client.roundTripTime == nil { try await Task.sleep(for: .milliseconds(10)) }
        return client.roundTripTime
    }
    #expect(latency != nil)

    try await client.sendBatch([[0xfe, 1], [0xfe, 2]], priority: .high)
    let first = try await expectWithin("first batch packet") { try await server.receive() }
    let snapshot = try await server.snapshot()
    #expect(snapshot.localAddress == server.localAddress)
    #expect(snapshot.remoteAddress == server.remoteAddress)
    #expect(snapshot.mtu == server.mtu)
    #expect(snapshot.bufferedPayloads == [[0xfe, 2]])
    #expect(snapshot.restore().isOpen)
    let second = try await expectWithin("second batch packet") { try await server.receive() }
    #expect(first == [0xfe, 1])
    #expect(second == [0xfe, 2])

    try await client.close()
    let afterClose = try await expectWithin("close") { try await server.receive() }
    #expect(afterClose == nil)
}

@Test func dialTimesOutOnSilentSocket() async throws {
    let silent = try UDPSocket(bindTo: IPv4Endpoint(address: 0x7f000001, port: 0))
    await #expect(throws: RakNetTransportError.timedOut) {
        try await RakNetConnection.dial(silent.localAddress, timeout: .milliseconds(100))
    }
}

@Test func dialDropsProbeSizeAfterThreeUnansweredAttempts() throws {
    let server = IPv4Endpoint(address: 0x7f000001, port: 19132)
    var attempt = DialAttempt(address: .ipv4(server), mtu: 1440, timeout: .seconds(5))
    for time in [UInt64(0), 300, 600] {
        let packet = try attempt.scheduledPacket(at: time)
        #expect(packet?.count == 1412)
    }
    let smaller = try attempt.scheduledPacket(at: 900)
    #expect(smaller?.count == 1172)
}

private struct DialTestTimeout: Error {
    let operation: String
}

private func expectWithin<T: Sendable>(
    _ operation: String, _ body: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask(operation: body)
        group.addTask {
            try await Task.sleep(for: .seconds(5))
            throw DialTestTimeout(operation: operation)
        }
        defer { group.cancelAll() }
        guard let result = try await group.next() else { throw DialTestTimeout(operation: operation) }
        return result
    }
}

@Test(.timeLimit(.minutes(1))) func listenerAdoptsDetachedSessionAndKeepsItsOrdering() async throws {
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: 0)
    let listener = try RakNetListener.listen(on: endpoint, maximumConnectionsPerIP: 1)
    defer { listener.close() }
    let client = try await RakNetConnection.dial(listener.localAddress)
    let incoming = try await expectWithin("accept") { try await listener.accept() }
    let server = try #require(incoming)

    try await server.send([0xfe, 0])
    let initial = try await expectWithin("reply before detach") { try await client.receive() }
    #expect(initial == [0xfe, 0])
    try await client.sendBatch([[0xfe, 1], [0xfe, 2]])
    let first = try await expectWithin("first batch packet") { try await server.receive() }
    #expect(first == [0xfe, 1])

    let detached = try await server.detach()
    let snapshot = try RakNetSessionSnapshot(data: detached.encoded())
    let ended = try await server.receive()
    #expect(ended == nil)
    let resumed = try await listener.adopt(snapshot)
    await #expect(throws: RakNetTransportError.connectionAlreadyExists) { try await listener.adopt(snapshot) }
    let buffered = try await expectWithin("buffered packet") { try await resumed.receive() }
    #expect(buffered == [0xfe, 2])

    try await server.close()
    try await client.send([0xfe, 3])
    let next = try await expectWithin("continued packet") { try await resumed.receive() }
    #expect(next == [0xfe, 3])
    try await resumed.send([0xfe, 4])
    let reply = try await expectWithin("reply after adopt") { try await client.receive() }
    #expect(reply == [0xfe, 4])
    try await client.close()
}

@Test(.timeLimit(.minutes(1))) func clientResumesDetachedSessionOnTheSamePort() async throws {
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: 0)
    let listener = try RakNetListener.listen(on: endpoint)
    defer { listener.close() }
    let client = try await RakNetConnection.dial(listener.localAddress)
    let incoming = try await expectWithin("accept") { try await listener.accept() }
    let server = try #require(incoming)

    try await server.sendBatch([[0xfe, 1], [0xfe, 2]])
    let first = try await expectWithin("first server packet") { try await client.receive() }
    #expect(first == [0xfe, 1])
    let detached = try await client.detach()
    let resumed = try await RakNetConnection.resume(detached)
    #expect(resumed.localAddress == server.remoteAddress)
    #expect(resumed.remoteAddress == listener.localAddress)
    let buffered = try await expectWithin("buffered server packet") { try await resumed.receive() }
    #expect(buffered == [0xfe, 2])

    try await resumed.send([0xfe, 3])
    let next = try await expectWithin("client packet after resume") { try await server.receive() }
    #expect(next == [0xfe, 3])
    try await resumed.close()
}
