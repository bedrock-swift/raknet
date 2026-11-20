public struct ConnectedSession: Sendable {
    public enum SessionError: Error, Equatable {
        case invalidMTU
        case invalidPayloadSize
        case invalidOrderingChannel
        case sendWindowFull
        case receiveWindowFull
        case retryLimit
    }

    public struct Input: Equatable, Sendable {
        public fileprivate(set) var replies: [[UInt8]] = []
        public fileprivate(set) var payloads: [[UInt8]] = []
    }

    private struct Pending: Sendable {
        let frame: Frame
        let sentAt: UInt64
        let retries: Int
    }

    private struct FragmentKey: Hashable, Sendable {
        let id: UInt16
        let order: Frame.Order?
        let sequence: UInt32?
    }

    private struct SequencedPayload: Sendable {
        let sequence: UInt32
        let payload: [UInt8]
    }

    private struct Assembly: Sendable {
        let count: UInt32
        var parts: [UInt32: [UInt8]] = [:]
        var bytes = 0
    }

    private static let windowSize = 1024
    private static let fragmentLimit = 128
    private static let receiveByteLimit = 1 << 20
    private static let sequenceMask: UInt32 = 0x00ffffff

    private let datagramLimit: Int
    private let peerDatagramLimit: Int
    private var nextDatagram: UInt32 = 0
    private var nextMessage: UInt32 = 0
    private var nextOrder = [UInt32](repeating: 0, count: 32)
    private var nextSequence = [UInt32](repeating: 0, count: 32)
    private var nextFragment: UInt16 = 0
    private var sent: [UInt32: Pending] = [:]
    private var retransmissionTimer = RetransmissionTimer()
    private var incoming = SequenceWindow(size: windowSize)
    private var reliable = SequenceWindow(size: windowSize)
    private var expectedOrder = [UInt32](repeating: 0, count: 32)
    private var ordered: [Frame.Order: [UInt8]] = [:]
    private var expectedSequence = [UInt32](repeating: 0, count: 32)
    private var futureSequences: [Frame.Order: SequencedPayload] = [:]
    private var fragments: [FragmentKey: Assembly] = [:]
    private var bufferedBytes = 0

    public init(mtu: UInt16) throws {
        guard OfflineWire.mtuRange.contains(mtu) else { throw SessionError.invalidMTU }
        datagramLimit = Int(mtu) - OfflineWire.ipv4UDPOverhead
        peerDatagramLimit = Int(mtu) // peer udp mtu convention
    }

    public mutating func send(_ payload: [UInt8], at milliseconds: UInt64) throws -> [[UInt8]] {
        try send(payload, reliability: .reliableOrdered, at: milliseconds)
    }

    public mutating func send(
        _ payload: [UInt8], reliability: RakNetReliability, channel: UInt8 = 0, at milliseconds: UInt64
    ) throws -> [[UInt8]] {
        guard channel < 32, reliability.hasOrder || channel == 0 else { throw SessionError.invalidOrderingChannel }
        var frameLimit = datagramLimit - 7 // datagram and frame headers
        if reliability.hasMessageIndex { frameLimit -= 3 }
        if reliability.hasSequenceIndex { frameLimit -= 3 }
        if reliability.hasOrder { frameLimit -= 4 }
        let fragmented = payload.count > frameLimit
        let chunkSize = fragmented ? frameLimit - 10 : frameLimit // split header
        let limit = reliability.hasMessageIndex ? Self.fragmentLimit * chunkSize : frameLimit
        guard !payload.isEmpty, payload.count <= limit else { throw SessionError.invalidPayloadSize }
        let count = (payload.count + chunkSize - 1) / chunkSize
        if reliability.hasMessageIndex, !canSend(count) { throw SessionError.sendWindowFull }

        let lane = Int(channel)
        let order = reliability.hasOrder ? Frame.Order(index: nextOrder[lane], channel: channel) : nil
        let sequence = reliability.hasSequenceIndex ? nextSequence[lane] : nil
        if reliability == .reliableOrdered {
            nextOrder[lane] = (nextOrder[lane] + 1) & Self.sequenceMask
            nextSequence[lane] = 0
        } else if let sequence {
            nextSequence[lane] = (sequence + 1) & Self.sequenceMask
        }

        let fragmentID = nextFragment
        if fragmented { nextFragment &+= 1 }
        var datagrams: [[UInt8]] = []
        datagrams.reserveCapacity(count)
        for index in 0..<count {
            let start = index * chunkSize
            let end = min(start + chunkSize, payload.count)
            let fragment = fragmented ? Frame.Fragment(count: UInt32(count), id: fragmentID, index: UInt32(index)) : nil
            let message = reliability.hasMessageIndex ? nextMessage : nil
            let frame = Frame(
                reliability: reliability, messageIndex: message, sequenceIndex: sequence, order: order,
                fragment: fragment, payload: Array(payload[start..<end])
            )
            if let message {
                nextMessage = (message + 1) & Self.sequenceMask
                datagrams.append(emit(frame, at: milliseconds, retries: 0))
            } else {
                datagrams.append(SessionDatagram.encode(sequence: takeSequence(), frame: frame))
            }
        }
        return datagrams
    }

    private func canSend(_ count: Int) -> Bool {
        sent.count + count <= Self.windowSize && sent.values.allSatisfy { packet in
            guard let index = packet.frame.messageIndex else { return true }
            let age = (nextMessage &- index) & Self.sequenceMask
            return Int(age) + count <= Self.windowSize
        }
    }

    public mutating func receive(_ datagram: [UInt8], at milliseconds: UInt64) throws -> Input {
        guard datagram.count <= peerDatagramLimit,
              let packet = SessionDatagram(datagram, sequenceLimit: Self.windowSize * 4) else { return Input() }

        switch packet {
        case let .acknowledgement(sequences):
            var sample: UInt64?
            for sequence in sequences {
                guard let packet = sent.removeValue(forKey: sequence), packet.retries == 0,
                      milliseconds >= packet.sentAt else { continue }
                let elapsed = milliseconds - packet.sentAt
                sample = min(sample ?? elapsed, elapsed) // one sample per acknowledgement
            }
            if let sample { retransmissionTimer.observe(sample) }
            return Input()
        case let .negativeAcknowledgement(sequences):
            return Input(replies: try resend(sequences, at: milliseconds))
        case let .frames(sequence, frames):
            let arrival = incoming.insert(sequence)
            guard arrival != .tooOld, arrival != .tooFar else { return Input() }
            var input = Input(replies: [SessionDatagram.acknowledgement(sequence)])
            guard case let .accepted(gap) = arrival else { return input }
            if gap > 0 {
                input.replies.append(SessionDatagram.negativeAcknowledgement(before: sequence, count: gap))
            }
            for frame in frames {
                try deliver(frame, into: &input.payloads)
            }
            return input
        }
    }

    public mutating func retransmit(at milliseconds: UInt64) throws -> [[UInt8]] {
        let expired = sent.compactMap { sequence, packet in
            let delay = retransmissionTimer.delay(after: packet.retries)
            return milliseconds >= packet.sentAt && milliseconds - packet.sentAt >= delay ? sequence : nil
        }
        return try resend(expired, at: milliseconds)
    }

    var hasPending: Bool { !sent.isEmpty }

    mutating func sendControl(_ payload: [UInt8]) -> [UInt8] {
        let frame = Frame(reliability: .unreliable, messageIndex: nil, order: nil, fragment: nil, payload: payload)
        return SessionDatagram.encode(sequence: takeSequence(), frame: frame)
    }

    private mutating func takeSequence() -> UInt32 {
        while sent[nextDatagram] != nil { nextDatagram = (nextDatagram + 1) & Self.sequenceMask }
        let sequence = nextDatagram
        nextDatagram = (sequence + 1) & Self.sequenceMask
        return sequence
    }

    private mutating func emit(_ frame: Frame, at time: UInt64, retries: Int) -> [UInt8] {
        let sequence = takeSequence()
        sent[sequence] = Pending(frame: frame, sentAt: time, retries: retries)
        return SessionDatagram.encode(sequence: sequence, frame: frame)
    }

    private mutating func resend(_ sequences: [UInt32], at time: UInt64) throws -> [[UInt8]] {
        guard sequences.allSatisfy({ (sent[$0]?.retries ?? 0) < 8 }) else { throw SessionError.retryLimit }
        let packets = sequences.compactMap { sent.removeValue(forKey: $0) }
        return packets.map { emit($0.frame, at: time, retries: $0.retries + 1) } // new datagram index
    }

    private mutating func deliver(_ frame: Frame, into payloads: inout [[UInt8]]) throws {
        if let message = frame.messageIndex {
            let arrival = reliable.insert(message)
            if arrival == .tooFar { throw SessionError.receiveWindowFull }
            guard case .accepted = arrival else { return }
        }

        guard let order = frame.order else {
            if let fragment = frame.fragment {
                guard frame.reliability.hasMessageIndex,
                      let payload = try assemble(fragment, order: nil, sequence: nil, payload: frame.payload) else { return }
                payloads.append(payload)
            } else {
                payloads.append(frame.payload)
            }
            return
        }
        let channel = Int(order.channel)
        let distance = (order.index &- expectedOrder[channel]) & Self.sequenceMask
        guard distance < 0x00800000 else { return }
        guard distance < Self.windowSize else { throw SessionError.receiveWindowFull }

        if let sequence = frame.sequenceIndex {
            let minimum = distance == 0
                ? expectedSequence[channel]
                : futureSequences[order].map { ($0.sequence + 1) & Self.sequenceMask } ?? 0
            guard Self.isCurrentOrNewer(sequence, than: minimum) else { return }
        } else if ordered[order] != nil {
            return
        }

        let payload: [UInt8]
        if let fragment = frame.fragment {
            guard frame.reliability.hasMessageIndex,
                  let assembled = try assemble(fragment, order: order, sequence: frame.sequenceIndex,
                                               payload: frame.payload) else { return }
            payload = assembled
        } else {
            payload = frame.payload
        }

        if let sequence = frame.sequenceIndex {
            try deliverSequenced(payload, index: sequence, order: order, future: distance != 0, into: &payloads)
        } else if distance != 0 {
            try reserve(payload.count)
            ordered[order] = payload
        } else {
            payloads.append(payload)
            advanceOrder(order, into: &payloads)
        }
    }

    private static func isCurrentOrNewer(_ sequence: UInt32, than minimum: UInt32) -> Bool {
        ((sequence &- minimum) & sequenceMask) < 0x00800000
    }

    private mutating func deliverSequenced(
        _ payload: [UInt8], index: UInt32, order: Frame.Order, future: Bool, into payloads: inout [[UInt8]]
    ) throws {
        let next = (index + 1) & Self.sequenceMask
        discardFragments(for: order, olderThan: next)
        if future {
            let currentSize = futureSequences[order]?.payload.count ?? 0
            try reserve(payload.count - currentSize) // replace older queued update
            futureSequences[order] = SequencedPayload(sequence: index, payload: payload)
        } else {
            expectedSequence[Int(order.channel)] = next
            payloads.append(payload)
        }
    }

    private mutating func advanceOrder(_ delivered: Frame.Order, into payloads: inout [[UInt8]]) {
        let channel = Int(delivered.channel)
        var order = delivered
        while true {
            discardFragments(for: order)
            expectedSequence[channel] = 0 // sequence resets at each ordered frame
            expectedOrder[channel] = (order.index + 1) & Self.sequenceMask
            order = Frame.Order(index: expectedOrder[channel], channel: delivered.channel)
            if let update = futureSequences.removeValue(forKey: order) {
                bufferedBytes -= update.payload.count
                expectedSequence[channel] = (update.sequence + 1) & Self.sequenceMask
                payloads.append(update.payload)
            }
            guard let queued = ordered.removeValue(forKey: order) else { return }
            bufferedBytes -= queued.count
            payloads.append(queued)
        }
    }

    private mutating func discardFragments(for order: Frame.Order, olderThan minimum: UInt32? = nil) {
        let stale = fragments.keys.filter { key in
            guard key.order == order else { return false }
            guard let minimum else { return true }
            guard let sequence = key.sequence else { return false }
            return !Self.isCurrentOrNewer(sequence, than: minimum)
        }
        for key in stale {
            if let assembly = fragments.removeValue(forKey: key) {
                bufferedBytes -= assembly.bytes
            }
        }
    }

    private mutating func assemble(
        _ fragment: Frame.Fragment, order: Frame.Order?, sequence: UInt32?, payload: [UInt8]
    ) throws -> [UInt8]? {
        guard fragment.count <= Self.fragmentLimit else { throw SessionError.receiveWindowFull }
        let key = FragmentKey(id: fragment.id, order: order, sequence: sequence)
        guard fragments[key] != nil || fragments.count < Self.fragmentLimit else {
            throw SessionError.receiveWindowFull
        }
        var assembly = fragments.removeValue(forKey: key) ?? Assembly(count: fragment.count)
        guard assembly.count == fragment.count, assembly.parts[fragment.index] == nil else {
            fragments[key] = assembly
            return nil
        }
        do {
            try reserve(payload.count)
        } catch {
            fragments[key] = assembly
            throw error
        }
        assembly.parts[fragment.index] = payload
        assembly.bytes += payload.count
        guard assembly.parts.count == Int(assembly.count) else {
            fragments[key] = assembly
            return nil
        }
        bufferedBytes -= assembly.bytes
        var result: [UInt8] = []
        result.reserveCapacity(assembly.bytes)
        for index in 0..<assembly.count {
            if let part = assembly.parts[index] { result += part }
        }
        return result
    }

    private mutating func reserve(_ count: Int) throws {
        guard count <= Self.receiveByteLimit - bufferedBytes else { throw SessionError.receiveWindowFull }
        bufferedBytes += count
    }
}

private struct SequenceWindow: Sendable {
    enum Arrival: Equatable {
        case tooOld
        case tooFar
        case duplicate
        case accepted(gap: Int)
    }

    private var newest: Int64 = -1
    private var seen: [Int64]

    init(size: Int) {
        seen = [Int64](repeating: -1, count: size)
    }

    mutating func insert(_ sequence: UInt32) -> Arrival {
        let mask: Int64 = 0x00ffffff
        let forward = (Int64(sequence) - newest) & mask // 24 bit wrap
        let delta = forward < 0x00800000 ? forward : forward - (mask + 1)
        guard delta <= seen.count else { return .tooFar }
        guard delta > -seen.count else { return .tooOld }
        let index = newest + delta
        guard index >= 0 else { return .tooOld }
        let slot = Int(index % Int64(seen.count))
        guard seen[slot] != index else { return .duplicate }
        seen[slot] = index
        newest = max(newest, index)
        return .accepted(gap: max(0, Int(delta) - 1))
    }
}
