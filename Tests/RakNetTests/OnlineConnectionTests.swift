import RakNet
import Testing

@Test func onlineHandshakeTransfersPayloadAndCloses() throws {
    let serverAddress = IPv4Endpoint(address: 0x7f000001, port: 19132)
    let clientAddress = IPv4Endpoint(address: 0x7f000001, port: 48000)
    let offlineServer = try OfflineServer(guid: 42, advertisement: [])
    var offlineClient = OfflineClient(server: serverAddress, guid: -123)
    let probe = try offlineClient.probe()
    let offer = try #require(offlineServer.reply(to: probe, from: clientAddress))
    guard case let .send(request) = try offlineClient.receive(offer, from: serverAddress) else {
        Issue.record("missing open request")
        return
    }
    let response = try #require(offlineServer.receive(request, from: clientAddress))
    let negotiation = try #require(response.negotiation)
    var server = try OnlineConnection(accepting: clientAddress, negotiation: negotiation)
    var client = try OnlineConnection(
        connectingTo: serverAddress, mtu: negotiation.mtu, clientGUID: offlineClient.guid
    )
    #expect(throws: OnlineConnection.ConnectionError.invalidState) {
        try client.send([0xfe], at: 0)
    }

    var acceptance: [[UInt8]] = []
    for datagram in try client.start(at: 1) {
        acceptance += try server.receive(datagram, from: clientAddress, at: 2).replies
    }
    var confirmation: [[UInt8]] = []
    for datagram in acceptance {
        confirmation += try client.receive(datagram, from: serverAddress, at: 3).replies
    }
    for datagram in confirmation {
        let input = try server.receive(datagram, from: clientAddress, at: 4)
        for reply in input.replies {
            _ = try client.receive(reply, from: serverAddress, at: 5)
        }
    }
    #expect(client.isOpen)
    #expect(server.isOpen)

    let data = [UInt8](repeating: 0xfe, count: 4096)
    var messages: [[UInt8]] = []
    for datagram in try client.send(data, at: 6).reversed() {
        let input = try server.receive(datagram, from: clientAddress, at: 7)
        messages += input.payloads
        for reply in input.replies where reply.first == 0xc0 {
            _ = try client.receive(reply, from: serverAddress, at: 8)
        }
    }
    #expect(messages == [data])

    let beforePing = try #require(client.roundTripTime)
    let keepalive = client.keepalive(at: 10)
    let ping = try #require(keepalive)
    let pingResult = try server.receive(ping, from: clientAddress, at: 50)
    let pong = try #require(pingResult.replies.first { $0.first == 0x84 })
    _ = try client.receive(pong, from: serverAddress, at: 90)
    #expect(try #require(client.roundTripTime) > beforePing)

    for datagram in try client.close(at: 100) {
        let input = try server.receive(datagram, from: clientAddress, at: 101)
        #expect(input.closed)
        for reply in input.replies {
            _ = try client.receive(reply, from: serverAddress, at: 102)
        }
    }
    #expect(client.isClosed)
    #expect(server.isClosed)
}
