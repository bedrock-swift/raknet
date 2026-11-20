import RakNet
import Testing

@Test func serverMultiplexesIndependentPeersWithoutSocketIO() throws {
    let endpoint = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 19132))
    let firstAddress = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 50001))
    let secondAddress = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 50002))
    let offline = try OfflineServer(guid: 42, advertisement: [])
    var server = try RakNetServer(on: endpoint, server: offline)
    var first = try simulateClient(firstAddress, connectingTo: endpoint, server: &server, at: 0)
    var second = try simulateClient(secondAddress, connectingTo: endpoint, server: &server, at: 100)

    let firstPackets = try first.send([0xfe, 1], at: 200)
    let secondPackets = try second.send([0xfe, 2], at: 201)
    let firstPacket = try #require(firstPackets.first)
    let secondPacket = try #require(secondPackets.first)
    let firstInput = server.receive(firstPacket, from: firstAddress, at: 202)
    let secondInput = server.receive(secondPacket, from: secondAddress, at: 203)
    #expect(firstInput.events == [.payload(firstAddress, [0xfe, 1])])
    #expect(secondInput.events == [.payload(secondAddress, [0xfe, 2])])

    let replies = try server.send([0xfe, 3], to: secondAddress, at: 204)
    let reply = try #require(replies.first)
    #expect(reply.destination == secondAddress)
    #expect(try second.receive(reply.bytes, from: endpoint, at: 205).payloads == [[0xfe, 3]])
    let timedOut = server.update(at: 15_203)
    #expect(Set(timedOut.events.compactMap { event in
        if case let .disconnected(address) = event { return address }
        return nil
    }) == [firstAddress, secondAddress])
}

private func simulateClient(
    _ address: RakNetEndpoint, connectingTo endpoint: RakNetEndpoint, server: inout RakNetServer, at time: UInt64
) throws -> OnlineConnection {
    var offline = OfflineClient(server: endpoint, guid: -Int64(address.port))
    let probe = try offline.probe()
    let challenge = server.receive(probe, from: address, at: time)
    let first = try #require(challenge.datagrams.first)
    guard case let .send(request) = try offline.receive(first.bytes, from: endpoint) else {
        throw OnlineConnection.ConnectionError.invalidState
    }
    let offer = server.receive(request, from: address, at: time + 1)
    let second = try #require(offer.datagrams.first)
    guard case let .negotiated(negotiation) = try offline.receive(second.bytes, from: endpoint) else {
        throw OnlineConnection.ConnectionError.invalidState
    }
    var client = try OnlineConnection(connectingTo: endpoint, mtu: negotiation.mtu, clientGUID: offline.guid)
    var toServer = try client.start(at: time + 2)
    for step in 3..<10 {
        let now = time + UInt64(step)
        var toClient: [RakNetServer.Datagram] = []
        for packet in toServer {
            let output = server.receive(packet, from: address, at: now)
            if output.events.contains(.connected(address, mtu: negotiation.mtu)) { return client }
            toClient += output.datagrams
        }
        toClient += server.update(at: now).datagrams
        toServer = []
        for packet in toClient where packet.destination == address {
            toServer += try client.receive(packet.bytes, from: endpoint, at: now).replies
        }
    }
    throw OnlineConnection.ConnectionError.invalidState
}

@Test func socketIndependentServerLimitsTrafficByIP() throws {
    let endpoint = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 19132))
    let first = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 50001))
    let second = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 50002))
    let other = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000002, port: 50001))
    let offline = try OfflineServer(guid: 42, advertisement: [])
    var server = try RakNetServer(on: endpoint, server: offline, offlinePacketLimits: .init(perIP: 1, total: 2))
    var client = OfflineClient(server: endpoint)
    let probe = try client.probe()
    let accepted = server.receive(probe, from: first, at: 0)
    let limited = server.receive(probe, from: second, at: 1)
    let otherAccepted = server.receive(probe, from: other, at: 2)
    #expect(accepted.datagrams.count == 1)
    #expect(limited.datagrams.isEmpty)
    #expect(otherAccepted.datagrams.count == 1)

    server.block(first, for: 200, at: 1_000)
    let blocked = server.receive(probe, from: second, at: 1_100)
    let resumed = server.receive(probe, from: second, at: 1_201)
    #expect(blocked.datagrams.isEmpty)
    #expect(resumed.datagrams.count == 1)
}
