@testable import RakNet
import Testing

@Test func reliableSequencedUsesBothWireIndices() throws {
    let frame = Frame(
        reliability: .reliableSequenced, messageIndex: 0x010203, sequenceIndex: 7,
        order: Frame.Order(index: 0, channel: 2), fragment: nil, payload: [0xfe, 7]
    )
    let bytes = SessionDatagram.encode(sequence: 0, frame: frame)
    #expect(bytes == [0x84, 0, 0, 0, 0x80, 0, 16, 3, 2, 1, 7, 0, 0, 0, 0, 0, 2, 0xfe, 7])

    var session = try ConnectedSession(mtu: 1440)
    let compatible = Frame(
        reliability: .reliableSequenced, messageIndex: 0, sequenceIndex: 7,
        order: Frame.Order(index: 0, channel: 2), fragment: nil, payload: [0xfe, 7]
    )
    let received = try session.receive(SessionDatagram.encode(sequence: 0, frame: compatible), at: 0)
    #expect(received.payloads == [[0xfe, 7]])
    let duplicate = Frame(
        reliability: .reliableSequenced, messageIndex: 0, sequenceIndex: 8,
        order: Frame.Order(index: 0, channel: 2), fragment: nil, payload: [0xfe, 8]
    )
    #expect(try session.receive(SessionDatagram.encode(sequence: 1, frame: duplicate), at: 1).payloads.isEmpty)
}

@Test func sequencedDeliveryKeepsLatestUpdateUntilItsOrderOpens() throws {
    var session = try ConnectedSession(mtu: 1440)
    func update(_ sequence: UInt32, order: UInt32, channel: UInt8 = 0) -> Frame {
        Frame(
            reliability: .unreliableSequenced, messageIndex: nil, sequenceIndex: sequence,
            order: Frame.Order(index: order, channel: channel), fragment: nil,
            payload: [0xfe, UInt8(sequence)]
        )
    }

    let updates = [update(4, order: 1), update(6, order: 1), update(5, order: 1)]
    for (index, frame) in updates.enumerated() {
        let received = try session.receive(SessionDatagram.encode(sequence: UInt32(index), frame: frame), at: 0)
        #expect(received.payloads.isEmpty)
    }

    let ordered = Frame(
        reliability: .reliableOrdered, messageIndex: 0, order: Frame.Order(index: 0, channel: 0),
        fragment: nil, payload: [0xfe, 0]
    )
    let opened = try session.receive(SessionDatagram.encode(sequence: 3, frame: ordered), at: 1)
    #expect(opened.payloads == [[0xfe, 0], [0xfe, 6]])

    let stale = try session.receive(SessionDatagram.encode(sequence: 4, frame: update(5, order: 1)), at: 2)
    #expect(stale.payloads.isEmpty)

    let otherChannel = try session.receive(
        SessionDatagram.encode(sequence: 5, frame: update(1, order: 0, channel: 1)), at: 3
    )
    #expect(otherChannel.payloads == [[0xfe, 1]])
}

@Test func fragmentedReliableSequencedUpdatesAreReassembled() throws {
    var session = try ConnectedSession(mtu: 1440)
    let order = Frame.Order(index: 0, channel: 0)
    for index in [UInt32(1), 0] {
        let part = Frame(
            reliability: .reliableSequenced, messageIndex: index, sequenceIndex: 2, order: order,
            fragment: Frame.Fragment(count: 2, id: 10, index: index), payload: [UInt8(index)]
        )
        let input = try session.receive(SessionDatagram.encode(sequence: index, frame: part), at: 0)
        #expect(input.payloads == (index == 0 ? [[0, 1]] : []))
    }
}
