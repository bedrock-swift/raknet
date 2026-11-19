import Dispatch
@testable import RakNet
import Testing

@Test func pingReportsAdvertisementAndLatency() async throws {
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: 0)
    let advertisement: [UInt8] = [0x4d, 0x43, 0x50, 0x45, 0x00, 0xff]
    let listener = try RakNetListener.listen(on: endpoint, guid: 42, advertisement: advertisement)
    defer { listener.close() }

    let pong = try await RakNetConnection.ping(listener.localAddress)
    #expect(pong.serverGUID == 42)
    #expect(pong.advertisement == advertisement)
    #expect(pong.roundTripTime >= .zero)
}

@Test func pingTimesOutOnSilentSocket() async throws {
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: 0)
    let silent = try UDPSocket(bindTo: endpoint)
    await #expect(throws: RakNetTransportError.timedOut) {
        try await RakNetConnection.ping(silent.localAddress, timeout: .milliseconds(100))
    }
}

@Test func pingRejectsMismatchedTimestampAndTruncatedAdvertisement() throws {
    let server = try OfflineServer(guid: 42, advertisement: [1, 2])
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: 50000)
    let request = PingExchange.request(timestamp: 7, guid: -42)
    let reply = try #require(server.reply(to: request, from: endpoint))

    #expect(PingExchange.response(reply, timestamp: 8, elapsed: .zero) == nil)
    #expect(PingExchange.response(Array(reply.dropLast()), timestamp: 7, elapsed: .zero) == nil)
    #expect(PingExchange.response(Array(reply.prefix(33)), timestamp: 7, elapsed: .zero)?.advertisement == [])
    #expect(PingExchange.response(reply, timestamp: 7, elapsed: .milliseconds(25))?.roundTripTime == .milliseconds(25))
}

@Test func activePingCanBeCancelled() async throws {
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: 0)
    let silent = try UDPSocket(bindTo: endpoint, receiveTimeout: 500)
    let pending = Task { try await RakNetConnection.ping(silent.localAddress) }
    defer { pending.cancel() }
    let request: (bytes: [UInt8], sender: IPv4Endpoint)? = try await withCheckedThrowingContinuation { continuation in
        DispatchQueue.global().async {
            var bytes = [UInt8](repeating: 0, count: 64)
            continuation.resume(with: Result { try silent.receive(into: &bytes) })
        }
    }
    try #require(request != nil)
    pending.cancel()
    await #expect(throws: CancellationError.self) {
        try await pending.value
    }
}
