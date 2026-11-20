import RakNet
import Testing

private let listener = IPv4Endpoint(address: 0x7f000001, port: 19132)
private let peer = IPv4Endpoint(address: 0x7f000001, port: 49153)

@Test func clientNegotiatesWithServer() throws {
    let server = try OfflineServer(guid: 42, advertisement: [], maximumMTU: 1200)
    var client = OfflineClient(server: listener, guid: 17)
    let probe = try client.probe()
    let first = try #require(server.reply(to: probe, from: peer))
    #expect(try client.receive(first, from: peer) == nil)
    guard case let .send(request) = try client.receive(first, from: listener) else {
        Issue.record("missing open request")
        return
    }
    #expect(client.pendingOpenRequest == request)
    let second = try #require(server.reply(to: request, from: peer))
    guard case let .negotiated(connection) = try client.receive(second, from: listener) else {
        Issue.record("missing negotiation")
        return
    }
    #expect(connection.serverGUID == 42)
    #expect(connection.clientAddress == .ipv4(peer))
    #expect(connection.mtu == 1200)
    #expect(client.pendingOpenRequest == nil)
    #expect(try client.receive(second, from: listener) == nil)
}

@Test func clientEchoesCookie() throws {
    let magic: [UInt8] = [
        0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe,
        0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78
    ]
    var client = OfflineClient(server: listener)
    _ = try client.probe(mtu: 1200)
    let first = [0x06] + magic + [0, 0, 0, 0, 0, 0, 0, 42, 1, 1, 2, 3, 4, 0x04, 0xb0]
    guard case let .send(request) = try client.receive(first, from: listener) else {
        Issue.record("missing cookie-bearing request")
        return
    }
    try #require(request.count == 39)
    #expect(request[31] & 0x80 == 0x80)
    #expect(Array(request[17..<22]) == [1, 2, 3, 4, 0])
    #expect(Array(request[22..<29]) == [4, 0x80, 0xff, 0xff, 0xfe, 0x4a, 0xbc])
}

@Test func clientReportsIncompatibleProtocol() throws {
    let server = try OfflineServer(guid: 42, advertisement: [], protocolVersion: 10)
    var client = OfflineClient(server: listener, guid: 17)
    let probe = try client.probe()
    let response = try #require(server.reply(to: probe, from: peer))
    #expect(throws: OfflineClient.HandshakeError.incompatibleProtocol(10)) {
        try client.receive(response, from: listener)
    }
}

@Test func ipv4MTUDefaultsTo1440() throws {
    let server = try OfflineServer(guid: 42, advertisement: [])
    var client = OfflineClient(server: listener)
    let probe = try client.probe()
    #expect(probe.count == 1412)
    let oversizedProbe = try client.probe(mtu: 1492)
    let reply = try #require(server.reply(to: oversizedProbe, from: peer))
    #expect(Array(reply.suffix(2)) == [0x05, 0xa0])
}

@Test func clientNegotiatesIPv6WithServer() throws {
    let listener = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 1, port: 19132))
    let peer = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0xfe80_0000_0000_0000, addressLow: 1, port: 49153, scopeID: 3))
    let server = try OfflineServer(guid: 42, advertisement: [], maximumMTU: 1300)
    var client = OfflineClient(server: listener, guid: -17)

    let probe = try client.probe()
    #expect(probe.count == 1392)
    let first = try #require(server.reply(to: probe, from: peer))
    guard case let .send(request) = try client.receive(first, from: listener) else {
        Issue.record("missing IPv6 open request")
        return
    }
    let encodedListener: [UInt8] = [6, 23, 0, 0x4a, 0xbc, 0, 0, 0, 0]
        + [UInt8](repeating: 0, count: 15) + [1, 0, 0, 0, 0]
    #expect(Array(request[17..<46]) == encodedListener)
    #expect(server.reply(to: Array(request.dropLast()), from: peer) == nil)

    let second = try #require(server.receive(request, from: peer))
    #expect(second.negotiation?.clientGUID == -17)
    #expect(second.datagram.count == 57)
    #expect(try client.receive(Array(second.datagram.dropLast()), from: listener) == nil)
    guard case let .negotiated(connection) = try client.receive(second.datagram, from: listener) else {
        Issue.record("missing IPv6 negotiation")
        return
    }
    #expect(connection.clientAddress == peer)
    #expect(connection.mtu == 1300)
}
