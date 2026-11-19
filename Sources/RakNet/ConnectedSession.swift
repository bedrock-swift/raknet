public struct ConnectedSession: Sendable {
    public enum SessionError: Error, Equatable {
        case invalidMTU
        case invalidPayloadSize
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
        let order: Frame.Order
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
    private var nextOrder: UInt32 = 0
    private var nextFragment: UInt16 = 0
    private var sent: [UInt32: Pending] = [:]
    private var incoming = SequenceWindow(size: windowSize)
    private var reliable = SequenceWindow(size: windowSize)
    private var expectedOrder = [UInt32](repeating: 0, count: 32)
    private var ordered: [Frame.Order: [UInt8]] = [:]
    private var fragments: [FragmentKey: Assembly] = [:]
    private var bufferedBytes = 0

    public init(mtu: UInt16) throws {
        guard OfflineWire.mtuRange.contains(mtu) else { throw SessionError.invalidMTU }
        datagramLimit = Int(mtu) - OfflineWire.ipv4UDPOverhead
        peerDatagramLimit = Int(mtu) // peer udp mtu convention
    }

    public mutating func send(_ payload: [UInt8], at milliseconds: UInt64) throws -> [[UInt8]] {
        let frameLimit = datagramLimit - 14
        let chunkSize = payload.count <= frameLimit ? frameLimit : frameLimit - 10 // split header
        guard !payload.isEmpty, payload.count <= Self.fragmentLimit * chunkSize else {
            throw SessionError.invalidPayloadSize
        }
        let count = (payload.count + chunkSize - 1) / chunkSize
        guard sent.count + count <= Self.windowSize,
              sent.values.allSatisfy({ packet in
                  guard let index = packet.frame.messageIndex else { return true }
                  let age = (nextMessage &- index) & Self.sequenceMask
                  return Int(age) + count <= Self.windowSize
              }) else { throw SessionError.sendWindowFull }

        let order = Frame.Order(index: nextOrder, channel: 0) // shared fragment order
        nextOrder = (nextOrder + 1) & Self.sequenceMask
        let fragmentID = nextFragment
        if count > 1 { nextFragment &+= 1 }
        var datagrams: [[UInt8]] = []
        datagrams.reserveCapacity(count)
        for index in 0..<count {
            let start = index * chunkSize
            let end = min(start + chunkSize, payload.count)
            let fragment = count == 1 ? nil : Frame.Fragment(count: UInt32(count), id: fragmentID, index: UInt32(index))
            let frame = Frame(
                reliability: .ordered, messageIndex: nextMessage, order: order,
                fragment: fragment, payload: Array(payload[start..<end])
            )
            nextMessage = (nextMessage + 1) & Self.sequenceMask
            datagrams.append(emit(frame, at: milliseconds, retries: 0))
        }
        return datagrams
    }

    public mutating func receive(_ datagram: [UInt8], at milliseconds: UInt64) throws -> Input {
        guard datagram.count <= peerDatagramLimit,
              let packet = SessionDatagram(datagram, sequenceLimit: Self.windowSize * 4) else { return Input() }

        switch packet {
        case let .acknowledgement(sequences):
            for sequence in sequences { sent.removeValue(forKey: sequence) }
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
            milliseconds >= packet.sentAt && milliseconds - packet.sentAt >= 300 ? sequence : nil
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
            if frame.fragment == nil { payloads.append(frame.payload) }
            return
        }
        let channel = Int(order.channel)
        let distance = (order.index &- expectedOrder[channel]) & Self.sequenceMask
        if distance >= 0x00800000 || ordered[order] != nil { return }
        guard distance < Self.windowSize else { throw SessionError.receiveWindowFull }

        let payload: [UInt8]
        if let fragment = frame.fragment {
            guard let assembled = try assemble(fragment, order: order, payload: frame.payload) else { return }
            payload = assembled
        } else {
            payload = frame.payload
        }

        if distance != 0 {
            try reserve(payload.count)
            ordered[order] = payload
            return
        }
        payloads.append(payload)
        expectedOrder[channel] = (order.index + 1) & Self.sequenceMask
        while let queued = ordered.removeValue(forKey: Frame.Order(index: expectedOrder[channel], channel: order.channel)) {
            bufferedBytes -= queued.count
            payloads.append(queued)
            expectedOrder[channel] = (expectedOrder[channel] + 1) & Self.sequenceMask
        }
    }

    private mutating func assemble(_ fragment: Frame.Fragment, order: Frame.Order, payload: [UInt8]) throws -> [UInt8]? {
        guard fragment.count <= Self.fragmentLimit else { throw SessionError.receiveWindowFull }
        let key = FragmentKey(id: fragment.id, order: order)
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
