@testable import RakNet
import Testing

@Test func statelessCookiesBindAnEndpointAndRotate() {
    let first = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 10001))
    let second = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 10002))
    let cookies = OfflineCookies(key0: 1, key1: 2)
    let value = cookies.issue(to: first, at: 1)
    for i in 0..<1024 {
        _ = cookies.issue(to: .ipv4(IPv4Endpoint(address: UInt32(i), port: 10001)), at: 2)
    }
    #expect(cookies.accepts(value, from: first, at: 4_999))
    #expect(cookies.accepts(value, from: first, at: 5_000))
    #expect(!cookies.accepts(value, from: second, at: 5_000))
    #expect(!cookies.accepts(value, from: first, at: 10_000))
}

@Test func cookieHashMatchesSipHash24Vectors() {
    let key0: UInt64 = 0x0706_0504_0302_0100
    let key1: UInt64 = 0x0f0e_0d0c_0b0a_0908
    #expect(OfflineCookies.tag([], key0: key0, key1: key1) == 0x726f_db47_dd0e_0e31)
    #expect(OfflineCookies.tag(Array(UInt8(0)..<15), key0: key0, key1: key1) == 0xa129_ca61_49be_45e5)
}

@Test func listenerRejectsInvalidCookieAndDifferentSourcePort() throws {
    let loopback = IPv4Endpoint(address: 0x7f000001, port: 0)
    let listener = try RakNetListener.listen(on: loopback)
    defer { listener.close() }

    let first = try UDPSocket(bindTo: loopback, receiveTimeout: 250)
    let second = try UDPSocket(bindTo: loopback, receiveTimeout: 250)
    var client = OfflineClient(server: listener.localAddress)
    var buffer = [UInt8](repeating: 0, count: 1500)
    try first.send(client.probe(), to: listener.localAddress)
    let reply = try first.receive(into: &buffer)
    let offer = try #require(reply)
    guard case let .send(request) = try client.receive(offer.bytes, from: offer.sender) else {
        Issue.record("missing cookie request")
        return
    }
    try #require(request.count == 39)

    var changed = request
    changed[17] ^= 1
    try first.send(changed, to: listener.localAddress)
    let badCookie = try first.receive(into: &buffer)
    #expect(badCookie == nil)

    try second.send(request, to: listener.localAddress)
    let wrongSource = try second.receive(into: &buffer)
    #expect(wrongSource == nil)

    try first.send(request, to: listener.localAddress)
    let response = try first.receive(into: &buffer)
    let accepted = try #require(response)
    guard case .negotiated = try client.receive(accepted.bytes, from: accepted.sender) else {
        Issue.record("cookie from original port rejected")
        return
    }
}

@Test func offlineHandshakeValidatesCookiesWithoutSocketIO() throws {
    let address = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 19132))
    let clientAddress = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 50000))
    let otherPort = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 50001))
    var client = OfflineClient(server: address, guid: -123)
    let server = try OfflineServer(guid: 42, advertisement: [])
    var handshake = OfflineServerHandshake(server: server)
    let probe = try client.probe()
    let challenge = handshake.receive(probe, from: clientAddress, at: 1)
    let response = try #require(challenge)
    guard case let .send(request) = try client.receive(response.datagram, from: address) else {
        Issue.record("missing cookie-bearing request")
        return
    }
    let refused = handshake.receive(request, from: otherPort, at: 2)
    #expect(refused == nil)
    let accepted = handshake.receive(request, from: clientAddress, at: 3)
    #expect(accepted?.negotiation?.clientGUID == -123)
}
