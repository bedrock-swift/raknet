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

@Test func sessionLearnsRetransmissionDelayFromAcknowledgements() throws {
    var sender = try ConnectedSession(mtu: 1440)
    _ = try sender.send([0xfe], at: 0)
    _ = try sender.receive([0xc0, 0, 1, 1, 0, 0, 0], at: 200)
    _ = try sender.send([0xfe], at: 200)

    #expect(try sender.retransmit(at: 799).isEmpty)
    #expect(try sender.retransmit(at: 800).count == 1)
    #expect(try sender.retransmit(at: 1999).isEmpty)
    #expect(try sender.retransmit(at: 2000).count == 1)
}

@Test func sessionIgnoresRTTFromRetransmittedPackets() throws {
    var sender = try ConnectedSession(mtu: 1440)
    _ = try sender.send([0xfe], at: 0)
    #expect(try sender.retransmit(at: 300).count == 1)
    _ = try sender.receive([0xc0, 0, 1, 1, 1, 0, 0], at: 301)

    _ = try sender.send([0xfe], at: 301)
    #expect(try sender.retransmit(at: 600).isEmpty)
    #expect(try sender.retransmit(at: 601).count == 1)
}

@Test func sessionBatchesAcknowledgementsIntoRanges() throws {
    var sender = try ConnectedSession(mtu: 400)
    var receiver = try ConnectedSession(mtu: 400, batchAcknowledgements: true)
    var packets: [[UInt8]] = []
    for _ in 0..<5 { packets += try sender.send([0xfe], at: 0) }
    for index in [0, 2, 2, 1, 4] {
        let input = try receiver.receive(packets[index], at: 1)
        #expect(input.replies.allSatisfy { $0.first == 0xa0 })
    }

    let flushed = receiver.flushAcknowledgements()
    let ack = try #require(flushed)
    #expect(ack == [0xc0, 0, 2, 0, 0, 0, 0, 2, 0, 0, 1, 4, 0, 0])
    #expect(receiver.flushAcknowledgements() == nil)
    _ = try sender.receive(ack, at: 2)
    #expect(try sender.retransmit(at: 300).count == 1)
}

@Test func sessionFlushesAcknowledgementsWithinMTU() throws {
    var receiver = try ConnectedSession(mtu: 400, batchAcknowledgements: true)
    var acknowledgements: [[UInt8]] = []
    for i in 0..<100 {
        let sequence = UInt8(i * 2)
        let packet: [UInt8] = [0x84, sequence, 0, 0, 0, 0, 8, 0xfe]
        let input = try receiver.receive(packet, at: 0)
        acknowledgements += input.replies.filter { $0.first == 0xc0 }
    }
    #expect(acknowledgements.count == 1)
    #expect(acknowledgements.first?.count == 371)
    let remainder = receiver.flushAcknowledgements()
    #expect(remainder?.count == 35)
}

@Test func sessionPacksFramesAndRetriesAMissingDatagram() throws {
    var sender = try ConnectedSession(mtu: 400)
    var receiver = try ConnectedSession(mtu: 400)
    let payloads: [[UInt8]] = [[UInt8](repeating: 0xfe, count: 400), [0xfd], [0xfc]]
    let packets = try sender.sendBatch(payloads, at: 0)
    try #require(packets.count == 2)
    #expect(packets.allSatisfy { $0.count <= 372 })

    let gap = try receiver.receive(packets[1], at: 1)
    #expect(gap.payloads.isEmpty)
    let nack = try #require(gap.replies.first { $0.first == 0xa0 })
    let retried = try sender.receive(nack, at: 2)
    let retry = try #require(retried.replies.first)
    #expect(Array(retry.dropFirst(4)) == Array(packets[0].dropFirst(4)))
    #expect(try receiver.receive(retry, at: 3).payloads == payloads)
    #expect(try receiver.receive(packets[0], at: 4).payloads.isEmpty)
}

@Test func invalidBatchDoesNotConsumeOrderingIndices() throws {
    var sender = try ConnectedSession(mtu: 400)
    #expect(throws: ConnectedSession.SessionError.invalidPayloadSize) {
        try sender.sendBatch([[0xfe], [], [0xfd]], at: 0)
    }
    var receiver = try ConnectedSession(mtu: 400)
    let packets = try sender.sendBatch([[0xfe], [0xfd]], at: 1)
    try #require(packets.count == 1)
    #expect(try receiver.receive(packets[0], at: 2).payloads == [[0xfe], [0xfd]])
}

@Test func congestionWindowReleasesQueuedDatagramsAfterAcknowledgement() throws {
    var sender = try ConnectedSession(mtu: 400, congestionControl: true)
    var receiver = try ConnectedSession(mtu: 400)
    let payloads = (1...4).map { [UInt8](repeating: UInt8($0), count: 358) }
    #expect(sender.sendCapacity == 372)
    let initial = try sender.sendBatch(payloads, at: 0)
    #expect(sender.sendCapacity == 0)
    try #require(initial.count == 1)
    #expect(try sender.retransmit(at: 299).isEmpty)
    let retry = try sender.retransmit(at: 300)
    try #require(retry.count == 1)
    let first = try receiver.receive(retry[0], at: 301)
    #expect(first.payloads == [payloads[0]])
    let ack = try #require(first.replies.first)
    let afterAck = try sender.receive(ack, at: 302)
    try #require(afterAck.replies.count == 2)
    let second = try receiver.receive(afterAck.replies[0], at: 303)
    #expect(second.payloads == [payloads[1]])
    let secondAck = try #require(second.replies.first)
    let further = try sender.receive(secondAck, at: 304)
    let last = try #require(further.replies.first)
    #expect(try receiver.receive(last, at: 305).payloads.isEmpty)
    #expect(try receiver.receive(afterAck.replies[1], at: 306).payloads == [payloads[2], payloads[3]])
}

@Test func sessionSchedulesPriorityWithoutChangingOrderedDelivery() throws {
    var sender = try ConnectedSession(mtu: 400)
    var receiver = try ConnectedSession(mtu: 400)
    #expect(try sender.sendBatch([[0xfe, 1]], priority: .low, at: 0).isEmpty)
    #expect(try sender.sendBatch([[0xfe, 2]], priority: .high, at: 0).isEmpty)
    let scheduled = try sender.update(at: 1)
    try #require(scheduled.count == 2)
    #expect(scheduled[0].suffix(2) == [0xfe, 2])
    #expect(try receiver.receive(scheduled[0], at: 2).payloads.isEmpty)
    #expect(try receiver.receive(scheduled[1], at: 3).payloads == [[0xfe, 1], [0xfe, 2]])
}

@Test func negotiatedMessageLimitsMatchTheWireBudget() throws {
    var sender = try ConnectedSession(mtu: 400)
    let maximum = sender.maximumMessageLength()
    #expect(maximum == 128 * 348)
    #expect(try sender.send([UInt8](repeating: 0xfe, count: maximum), at: 0).count == 128)
    #expect(throws: ConnectedSession.SessionError.invalidPayloadSize) {
        try sender.send([UInt8](repeating: 0xfe, count: maximum + 1), at: 0)
    }
    let unsequenced = sender.maximumMessageLength(for: .unreliable)
    #expect(unsequenced == 365)
    let packet = try sender.send([UInt8](repeating: 0xfe, count: unsequenced), reliability: .unreliable, at: 0)
    #expect(packet.first?.count == 372)
}
