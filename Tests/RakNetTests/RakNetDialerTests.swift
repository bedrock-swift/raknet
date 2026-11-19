@testable import RakNet
import Testing

@Test(.timeLimit(.minutes(1))) func dialExchangesPayloadAndCloses() async throws {
    let address = IPv4Endpoint(address: 0x7f000001, port: 0)
    let listener = try RakNetListener.listen(on: address)
    defer { listener.close() }

    let client = try await RakNetConnection.dial(listener.localAddress)
    let incoming = try await listener.accept()
    let server = try #require(incoming)
    #expect(client.remoteAddress == listener.localAddress)
    #expect(server.remoteAddress == client.localAddress)
    #expect(client.mtu == IPv4MTU.defaultValue)

    let payload = [UInt8](repeating: 0xfe, count: 4096)
    try await client.send(payload)
    let request = try await server.receive()
    #expect(request == payload)
    try await server.send([0xfe, 0x42], reliability: .unreliableSequenced, channel: 3)
    let reply = try await client.receive()
    #expect(reply == [0xfe, 0x42])

    try await client.close()
    let afterClose = try await server.receive()
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
    var attempt = DialAttempt(address: server, mtu: 1440, timeout: .seconds(5))
    for time in [UInt64(0), 300, 600] {
        let packet = try attempt.scheduledPacket(at: time)
        #expect(packet?.count == 1412)
    }
    let smaller = try attempt.scheduledPacket(at: 900)
    #expect(smaller?.count == 1172)
}
