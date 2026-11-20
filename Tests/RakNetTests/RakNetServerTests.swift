@testable import RakNet
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
    _ address: RakNetEndpoint, connectingTo endpoint: RakNetEndpoint, server: inout RakNetServer, at time: UInt64,
    replacing: Bool = false
) throws -> OnlineConnection {
    var offline = OfflineClient(server: endpoint, guid: -Int64(address.port))
    let probe = try offline.probe()
    let challenge = server.receive(probe, from: address, at: time)
    let first = try #require(challenge.datagrams.first)
    guard case let .send(request) = try offline.receive(first.bytes, from: endpoint) else {
        throw OnlineConnection.ConnectionError.invalidState
    }
    let offer = server.receive(request, from: address, at: time + 1)
    let retry = server.receive(request, from: address, at: time + 1)
    #expect(retry.datagrams == offer.datagrams)
    #expect(retry.events.isEmpty)
    #expect(offer.events == (replacing ? [.disconnected(address)] : []))
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
            if output.events.contains(.connected(address, mtu: negotiation.mtu)) {
                let stale = server.receive(request, from: address, at: now)
                #expect(stale.datagrams.isEmpty)
                return client
            }
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

@Test func pendingHandshakeDoesNotConsumeEstablishedCapacity() throws {
    let endpoint = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 19132))
    let stalled = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 50001))
    let sameHost = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 50002))
    let otherHost = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000002, port: 50003))
    let offline = try OfflineServer(guid: 42, advertisement: [])
    var server = try RakNetServer(
        on: endpoint, server: offline, maximumConnections: 1,
        pendingConnections: RakNetPendingConnectionLimits(total: 2, perIP: 1), useCookies: false
    )
    var opening = OfflineClient(server: endpoint, guid: -10)
    let probe = try opening.probe()
    let offered = server.receive(probe, from: stalled, at: 0)
    let reply = try #require(offered.datagrams.first)
    guard case let .send(request) = try opening.receive(reply.bytes, from: endpoint) else {
        Issue.record("missing request")
        return
    }

    let pending = server.receive(request, from: stalled, at: 1)
    #expect(pending.datagrams.count == 1)
    let same = server.receive(request, from: sameHost, at: 2)
    #expect(same.datagrams.isEmpty)
    let connected = try simulateClient(otherHost, connectingTo: endpoint, server: &server, at: 4)
    #expect(connected.isOpen)
}

@Test func serverProbesOnlyAfterAQuietPeriod() throws {
    let endpoint = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 19132))
    let remote = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 50001))
    let offline = try OfflineServer(guid: 42, advertisement: [])
    var server = try RakNetServer(on: endpoint, server: offline)
    var client = try simulateClient(remote, connectingTo: endpoint, server: &server, at: 0)
    let packets = try client.send([0xfe], at: 2_100)
    let packet = try #require(packets.first)
    _ = server.receive(packet, from: remote, at: 2_100)
    let active = server.update(at: 2_101)
    #expect(!active.datagrams.contains { $0.bytes.count == 16 && $0.bytes[7] == 0 })
    let idle = server.update(at: 4_100)
    #expect(idle.datagrams.contains { $0.bytes.count == 16 && $0.bytes[7] == 0 })
}

@Test func listenerLimitsGroupIPv6AddressesByPrefix() throws {
    let first = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0x2001_0db8_0000_0001, addressLow: 1, port: 2000))
    let samePrefix = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0x2001_0db8_0000_0001, addressLow: 2, port: 2001))
    let otherPrefix = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0x2001_0db8_0000_0002, addressLow: 1, port: 2000))

    var rate = OfflinePacketRate(limits: RakNetOfflinePacketLimits(perIP: 1, total: 3))
    let allowed = [first, samePrefix, otherPrefix].map { rate.allow($0, at: 0) }
    #expect(allowed == [true, false, true])

    var admission = try ConnectionAdmission(maximum: 3, perIP: 1, pending: .init(total: 3, perIP: 1))
    try admission.reserve(first, established: false)
    #expect(throws: RakNetTransportError.connectionLimitReached) { try admission.reserve(samePrefix, established: false) }
    try admission.promote(first)
    #expect(throws: RakNetTransportError.connectionLimitReached) { try admission.reserve(samePrefix, established: true) }
    try admission.reserve(otherPrefix, established: true)

    let access = ListenerAccess()
    access.block(first, for: .seconds(10))
    #expect(access.isBlocked(samePrefix))
    #expect(!access.isBlocked(otherPrefix))
    access.unblock(samePrefix)
    #expect(!access.isBlocked(first))
}

@Test func listenerLimitsRecognizeIPv4MappedAddressesAndIPv6Scopes() {
    let v4 = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f00_0001, port: 2000))
    let mapped = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 0xffff_7f00_0001, port: 2001))
    #expect(ListenerHost(v4) == ListenerHost(mapped))

    let link = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0xfe80_0000_0000_0000, addressLow: 1, port: 2000, scopeID: 1))
    let otherLink = RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0xfe80_0000_0000_0000, addressLow: 1, port: 2000, scopeID: 2))
    #expect(ListenerHost(link) != ListenerHost(otherLink))
}

@Test func serverReplacesAnOpenSessionAfterAFreshCookieExchange() throws {
    let endpoint = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 19132))
    let address = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 50001))
    let offline = try OfflineServer(guid: 42, advertisement: [])
    var server = try RakNetServer(on: endpoint, server: offline, maximumConnections: 1)
    let first = try simulateClient(address, connectingTo: endpoint, server: &server, at: 0)
    #expect(first.isOpen)

    var second = try simulateClient(address, connectingTo: endpoint, server: &server, at: 100, replacing: true)
    let packets = try second.send([0xfe, 42], at: 200)
    let packet = try #require(packets.first)
    let received = server.receive(packet, from: address, at: 201)
    #expect(received.events == [.payload(address, [0xfe, 42])])
}
