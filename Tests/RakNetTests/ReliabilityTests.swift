import RakNet
import Testing

@Test func unreliablePayloadIsNeverRetransmitted() throws {
    var sender = try ConnectedSession(mtu: 400)
    var receiver = try ConnectedSession(mtu: 400)
    let packets = try sender.send([0xfe, 1], reliability: .unreliable, at: 0)
    let first = try #require(packets.first)
    #expect(first[4] == 0)
    #expect(try receiver.receive(first, at: 1).payloads == [[0xfe, 1]])
    #expect(try sender.retransmit(at: 1000).isEmpty)

    #expect(throws: ConnectedSession.SessionError.invalidPayloadSize) {
        try sender.send([UInt8](repeating: 0xfe, count: 366), reliability: .unreliable, at: 0)
    }
    #expect(throws: ConnectedSession.SessionError.invalidOrderingChannel) {
        try sender.send([0xfe], reliability: .unreliable, channel: 1, at: 0)
    }
}

@Test func reliableUnorderedPayloadCanFragment() throws {
    var sender = try ConnectedSession(mtu: 1440)
    var receiver = try ConnectedSession(mtu: 1440)
    let payload = [UInt8](repeating: 0xfe, count: 4096)
    let packets = try sender.send(payload, reliability: .reliable, at: 0)
    #expect(packets.count == 3)
    #expect(packets.allSatisfy { $0.count <= 1412 && $0[4] == 0x50 })
    var received: [[UInt8]] = []
    for packet in packets.reversed() {
        received += try receiver.receive(packet, at: 1).payloads
    }
    #expect(received == [payload])
    #expect(try sender.retransmit(at: 300).count == 3)
}

@Test func sequencedSendUsesPerChannelOrdering() throws {
    var sender = try ConnectedSession(mtu: 1440)
    var receiver = try ConnectedSession(mtu: 1440)
    let old = try #require(sender.send([0xfe, 1], reliability: .unreliableSequenced, channel: 3, at: 0).first)
    let latest = try #require(sender.send([0xfe, 2], reliability: .reliableSequenced, channel: 3, at: 0).first)
    let ordered = try #require(sender.send([0xfe, 3], reliability: .reliableOrdered, channel: 3, at: 0).first)
    let next = try #require(sender.send([0xfe, 4], reliability: .unreliableSequenced, channel: 3, at: 0).first)
    #expect(try receiver.receive(latest, at: 1).payloads == [[0xfe, 2]])
    #expect(try receiver.receive(old, at: 1).payloads.isEmpty)
    #expect(try receiver.receive(next, at: 1).payloads.isEmpty)
    #expect(try receiver.receive(ordered, at: 1).payloads == [[0xfe, 3], [0xfe, 4]])
}
