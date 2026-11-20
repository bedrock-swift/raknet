public enum RakNetReliability: UInt8, Sendable {
    case unreliable = 0
    case unreliableSequenced = 1
    case reliable = 2
    case reliableOrdered = 3
    case reliableSequenced = 4
    case unreliableWithAckReceipt = 5
    case reliableWithAckReceipt = 6
    case reliableOrderedWithAckReceipt = 7

    var hasMessageIndex: Bool {
        self == .reliable || self == .reliableSequenced || self == .reliableWithAckReceipt || isOrdered
    }

    var hasSequenceIndex: Bool {
        self == .unreliableSequenced || self == .reliableSequenced
    }

    var isOrdered: Bool {
        self == .reliableOrdered || self == .reliableOrderedWithAckReceipt
    }

    var hasOrder: Bool {
        isOrdered || hasSequenceIndex
    }
}

struct Frame: Equatable, Sendable {
    struct Order: Equatable, Hashable, Sendable {
        let index: UInt32
        let channel: UInt8
    }

    struct Fragment: Equatable, Sendable {
        let count: UInt32
        let id: UInt16
        let index: UInt32
    }

    let reliability: RakNetReliability
    let messageIndex: UInt32?
    let sequenceIndex: UInt32?
    let order: Order?
    let fragment: Fragment?
    let payload: [UInt8]

    init(
        reliability: RakNetReliability, messageIndex: UInt32?, sequenceIndex: UInt32? = nil,
        order: Order?, fragment: Fragment?, payload: [UInt8]
    ) {
        self.reliability = reliability
        self.messageIndex = messageIndex
        self.sequenceIndex = sequenceIndex
        self.order = order
        self.fragment = fragment
        self.payload = payload
    }

    init?(reading bytes: inout DatagramReader) {
        guard let header = bytes.byte(),
              let reliability = RakNetReliability(rawValue: header >> 5),
              let bits = bytes.integer(UInt16.self), bits != 0 else { return nil }

        let messageIndex: UInt32?
        if !reliability.hasMessageIndex {
            messageIndex = nil
        } else {
            guard let index = bytes.sequence() else { return nil }
            messageIndex = index
        }

        let sequenceIndex: UInt32?
        if reliability.hasSequenceIndex {
            guard let index = bytes.sequence() else { return nil }
            sequenceIndex = index
        } else {
            sequenceIndex = nil
        }

        let order: Order?
        if reliability.hasOrder {
            guard let index = bytes.sequence(), let channel = bytes.byte(), channel < 32 else { return nil }
            order = Order(index: index, channel: channel)
        } else {
            order = nil
        }

        let fragment: Fragment?
        if header & 0x10 != 0 {
            guard let count = bytes.integer(UInt32.self), count > 1,
                  let id = bytes.integer(UInt16.self),
                  let index = bytes.integer(UInt32.self), index < count else { return nil }
            fragment = Fragment(count: count, id: id, index: index)
        } else {
            fragment = nil
        }
        guard let payload = bytes.read((Int(bits) + 7) / 8) else { return nil } // bit length rounding
        self.init(
            reliability: reliability, messageIndex: messageIndex, sequenceIndex: sequenceIndex,
            order: order, fragment: fragment, payload: Array(payload)
        )
    }

    var encodedSize: Int {
        3 + (messageIndex == nil ? 0 : 3) + (sequenceIndex == nil ? 0 : 3)
            + (order == nil ? 0 : 4) + (fragment == nil ? 0 : 10) + payload.count
    }

    func encode(into bytes: inout [UInt8]) {
        var header = reliability.rawValue << 5
        if fragment != nil { header |= 0x10 }
        bytes.append(header)
        bytes += bigEndianBytes(UInt16(payload.count * 8))
        if let messageIndex { bytes.appendSequence(messageIndex) }
        if let sequenceIndex { bytes.appendSequence(sequenceIndex) }
        if let order {
            bytes.appendSequence(order.index)
            bytes.append(order.channel)
        }
        if let fragment {
            bytes += bigEndianBytes(fragment.count)
            bytes += bigEndianBytes(fragment.id)
            bytes += bigEndianBytes(fragment.index)
        }
        bytes += payload
    }
}

enum SessionDatagram {
    static let frameHeaderSize = 4

    case frames(UInt32, [Frame])
    case acknowledgement([UInt32])
    case negativeAcknowledgement([UInt32])

    init?(_ datagram: [UInt8], sequenceLimit: Int) {
        var bytes = DatagramReader(datagram)
        guard let header = bytes.byte(), header & 0x80 != 0 else { return nil }
        switch header & 0x60 {
        case 0x00:
            guard let sequence = bytes.sequence() else { return nil }
            var frames: [Frame] = []
            while !bytes.finished {
                guard let frame = Frame(reading: &bytes) else { return nil }
                frames.append(frame)
            }
            guard !frames.isEmpty else { return nil }
            self = .frames(sequence, frames)
        case 0x20, 0x40:
            guard let records = bytes.integer(UInt16.self), records > 0 else { return nil }
            var sequences: [UInt32] = []
            for _ in 0..<records {
                guard let kind = bytes.byte(), let first = bytes.sequence() else { return nil }
                switch kind {
                case 0:
                    guard let last = bytes.sequence(), last >= first,
                          Int(last - first) + 1 <= sequenceLimit - sequences.count else { return nil }
                    sequences += first...last
                case 1:
                    guard sequences.count < sequenceLimit else { return nil }
                    sequences.append(first)
                default:
                    return nil
                }
            }
            guard bytes.finished else { return nil }
            self = header & 0x40 != 0 ? .acknowledgement(sequences) : .negativeAcknowledgement(sequences)
        default:
            return nil
        }
    }

    static func encode(sequence: UInt32, frame: Frame) -> [UInt8] {
        encode(sequence: sequence, frames: [frame])
    }

    static func encode(sequence: UInt32, frames: [Frame]) -> [UInt8] {
        var bytes: [UInt8] = [0x84]
        bytes.appendSequence(sequence)
        for frame in frames { frame.encode(into: &bytes) }
        return bytes
    }

    static func negativeAcknowledgement(before sequence: UInt32, count: Int) -> [UInt8] {
        let mask: UInt32 = 0x00ffffff
        let first = (sequence &- UInt32(count)) & mask
        let last = (sequence &- 1) & mask
        var bytes: [UInt8] = [0xa0, 0, first <= last ? 1 : 2]
        if first <= last {
            bytes.appendRecord(first, last)
        } else {
            bytes.appendRecord(first, mask)
            bytes.appendRecord(0, last)
        }
        return bytes
    }

    static func acknowledgement(_ sequence: UInt32) -> [UInt8] {
        var bytes: [UInt8] = [0xc0, 0, 1]
        bytes.appendRecord(sequence, sequence)
        return bytes
    }

    static func acknowledgement(_ sequences: Set<UInt32>) -> [UInt8]? {
        let sorted = sequences.sorted()
        guard let start = sorted.first else { return nil }
        var bytes: [UInt8] = [0xc0, 0, 0]
        var records: UInt16 = 1
        var first = start
        var last = start
        for sequence in sorted.dropFirst() {
            if sequence != last + 1 {
                bytes.appendRecord(first, last)
                records += 1
                first = sequence
            }
            last = sequence
        }
        bytes.appendRecord(first, last)
        bytes.replaceSubrange(1..<3, with: bigEndianBytes(records))
        return bytes
    }
}

struct DatagramReader {
    private let bytes: [UInt8]
    private var offset = 0

    init(_ bytes: [UInt8]) {
        self.bytes = bytes
    }

    var remaining: Int { bytes.count - offset }
    var finished: Bool { remaining == 0 }

    mutating func read(_ count: Int) -> ArraySlice<UInt8>? {
        guard count <= bytes.count - offset else { return nil }
        let start = offset
        offset += count
        return bytes[start..<offset]
    }

    mutating func byte() -> UInt8? {
        read(1)?.first
    }

    mutating func integer<T: FixedWidthInteger>(_ type: T.Type) -> T? {
        read(MemoryLayout<T>.size).map { T(wireBytes: $0) }
    }

    mutating func sequence() -> UInt32? {
        guard let bytes = read(3) else { return nil } // little endian index
        let i = bytes.startIndex
        return UInt32(bytes[i]) | UInt32(bytes[i + 1]) << 8 | UInt32(bytes[i + 2]) << 16
    }
}

private extension Array where Element == UInt8 {
    mutating func appendRecord(_ first: UInt32, _ last: UInt32) {
        append(first == last ? 1 : 0)
        appendSequence(first)
        if first != last { appendSequence(last) }
    }

    mutating func appendSequence(_ value: UInt32) {
        append(UInt8(truncatingIfNeeded: value))
        append(UInt8(truncatingIfNeeded: value >> 8))
        append(UInt8(truncatingIfNeeded: value >> 16))
    }
}
