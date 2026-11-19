import RakNet
import Testing

@Test func sessionEncodesAndAcknowledgesReliablePayload() throws {
    var sender = try ConnectedSession(mtu: 1440)
    var receiver = try ConnectedSession(mtu: 1440)
    let datagrams = try sender.send([0xfe, 0x42], at: 0)
    #expect(datagrams == [[0x84, 0, 0, 0, 0x60, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0xfe, 0x42]])
    let received = try receiver.receive(try #require(datagrams.first), at: 1)
    #expect(received.payloads == [[0xfe, 0x42]])
    #expect(received.replies == [[0xc0, 0, 1, 1, 0, 0, 0]])
    _ = try sender.receive(try #require(received.replies.first), at: 2)
    #expect(try sender.retransmit(at: 300).isEmpty)
}

@Test func sessionRetriesAfterLossAndDeliversInOrder() throws {
    var sender = try ConnectedSession(mtu: 1440)
    var receiver = try ConnectedSession(mtu: 1440)
    let first = try #require(sender.send([0xfe, 1], at: 0).first)
    let second = try #require(sender.send([0xfe, 2], at: 0).first)
    let gap = try receiver.receive(second, at: 1)
    #expect(gap.payloads.isEmpty)
    #expect(gap.replies == [[0xc0, 0, 1, 1, 1, 0, 0], [0xa0, 0, 1, 1, 0, 0, 0]])
    for reply in gap.replies {
        let retried = try sender.receive(reply, at: 2)
        for packet in retried.replies {
            #expect(Array(packet[1..<4]) == [2, 0, 0])
            let recovered = try receiver.receive(packet, at: 3)
            #expect(recovered.payloads == [[0xfe, 1], [0xfe, 2]])
            for reply in recovered.replies {
                _ = try sender.receive(reply, at: 4)
            }
        }
    }
    #expect(try receiver.receive(first, at: 5).payloads.isEmpty)
    #expect(try sender.retransmit(at: 400).isEmpty)
}

@Test func sessionReassemblesOutOfOrderFragmentsWithinMTU() throws {
    var sender = try ConnectedSession(mtu: 1440)
    var receiver = try ConnectedSession(mtu: 1440)
    let payload = (0..<4096).map { UInt8(truncatingIfNeeded: $0) }
    let packets = try sender.send(payload, at: 0)
    #expect(packets.count == 3)
    #expect(packets.allSatisfy { $0.count <= 1412 })
    var received: [[UInt8]] = []
    for packet in packets.reversed() {
        received += try receiver.receive(packet, at: 1).payloads
    }
    #expect(received == [payload])
}

@Test func sessionRetransmitsWhenAcknowledgementIsLost() throws {
    var sender = try ConnectedSession(mtu: 1440)
    var receiver = try ConnectedSession(mtu: 1440)
    let original = try #require(sender.send([0xfe], at: 4).first)
    #expect(try receiver.receive(original, at: 5).payloads == [[0xfe]])
    #expect(try sender.retransmit(at: 303).isEmpty)
    let retry = try #require(sender.retransmit(at: 304).first)
    let duplicate = try receiver.receive(retry, at: 305)
    #expect(duplicate.payloads.isEmpty)
    #expect(duplicate.replies == [[0xc0, 0, 1, 1, 1, 0, 0]])
}
