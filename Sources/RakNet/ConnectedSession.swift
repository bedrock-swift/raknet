public enum RakNetPriority: Int, CaseIterable, Sendable {
    case immediate
    case high
    case normal
    case low
}

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

    private struct Outbound: Sendable {
        let frames: [Frame]
        let size: Int
    }

    private struct OutboundQueue: Sendable {
        private var datagrams: [Outbound] = []
        private var offset = 0

        var first: Outbound? {
            offset < datagrams.count ? datagrams[offset] : nil
        }

        var firstReliable: Outbound? {
            datagrams[offset...].first { $0.frames.first?.messageIndex != nil }
        }

        mutating func append(_ new: [Outbound]) {
            datagrams += new
        }

        mutating func dropFirst() {
            offset += 1
            if offset == datagrams.count {
                datagrams.removeAll(keepingCapacity: true)
                offset = 0
            } else if offset >= datagrams.count - offset {
                datagrams.removeFirst(offset)
                offset = 0
            }
        }
    }

    private struct Pending: Sendable {
        let datagram: Outbound
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
    private let batchAcknowledgements: Bool
    private var pendingAcknowledgements: Set<UInt32> = []
    private var nextDatagram: UInt32 = 0
    private var nextMessage: UInt32 = 0
    private var nextOrder = [UInt32](repeating: 0, count: 32)
    private var nextSequence = [UInt32](repeating: 0, count: 32)
    private var nextFragment: UInt16 = 0
    private var sent: [UInt32: Pending] = [:]
    private var bytesInFlight = 0
    private var congestion: CongestionWindow?
    private var outbound = RakNetPriority.allCases.map { _ in OutboundQueue() }
    private var queuedFrames = 0
    private var retransmissionTimer = RetransmissionTimer()
    private var incoming = SequenceWindow(size: windowSize)
    private var reliable = SequenceWindow(size: windowSize)
    private var expectedOrder = [UInt32](repeating: 0, count: 32)
    private var ordered: [Frame.Order: [UInt8]] = [:]
    private var expectedSequence = [UInt32](repeating: 0, count: 32)
    private var futureSequences: [Frame.Order: SequencedPayload] = [:]
    private var fragments: [FragmentKey: Assembly] = [:]
    private var bufferedBytes = 0

    public init(mtu: UInt16, batchAcknowledgements: Bool = false, congestionControl: Bool = false) throws {
        try self.init(
            mtu: mtu, udpOverhead: OfflineWire.ipv4UDPOverhead,
            batchAcknowledgements: batchAcknowledgements, congestionControl: congestionControl
        )
    }

    init(mtu: UInt16, udpOverhead: Int, batchAcknowledgements: Bool, congestionControl: Bool) throws {
        guard OfflineWire.mtuRange.contains(mtu) else { throw SessionError.invalidMTU }
        datagramLimit = Int(mtu) - udpOverhead
        peerDatagramLimit = Int(mtu) // peer udp mtu convention
        self.batchAcknowledgements = batchAcknowledgements
        congestion = congestionControl ? CongestionWindow(datagramSize: datagramLimit) : nil
    }

    public var roundTripTime: Duration? {
        retransmissionTimer.smoothed.map { .milliseconds(Int64($0)) }
    }

    mutating func observeRoundTripTime(_ milliseconds: UInt64) {
        retransmissionTimer.observe(milliseconds)
    }

    public mutating func send(_ payload: [UInt8], at milliseconds: UInt64) throws -> [[UInt8]] {
        try send(payload, reliability: .reliableOrdered, at: milliseconds)
    }

    public mutating func send(
        _ payload: [UInt8], reliability: RakNetReliability, channel: UInt8 = 0,
        priority: RakNetPriority = .immediate, at milliseconds: UInt64
    ) throws -> [[UInt8]] {
        try sendBatch([payload], reliability: reliability, channel: channel, priority: priority, at: milliseconds)
    }

    public mutating func sendBatch(
        _ payloads: [[UInt8]], reliability: RakNetReliability = .reliableOrdered, channel: UInt8 = 0,
        priority: RakNetPriority = .immediate, at milliseconds: UInt64
    ) throws -> [[UInt8]] {
        guard channel < 32, reliability.hasOrder || channel == 0 else { throw SessionError.invalidOrderingChannel }
        var frameLimit = datagramLimit - SessionDatagram.frameHeaderSize - 3 // frame header
        if reliability.hasMessageIndex { frameLimit -= 3 }
        if reliability.hasSequenceIndex { frameLimit -= 3 }
        if reliability.hasOrder { frameLimit -= 4 }

        let enqueue = priority != .immediate || (congestion != nil && reliability.hasMessageIndex)
        var frameCount = 0
        let chunkSizes = try payloads.map { payload in
            let chunkSize = payload.count > frameLimit ? frameLimit - 10 : frameLimit // split header
            let limit = reliability.hasMessageIndex ? Self.fragmentLimit * chunkSize : frameLimit
            guard !payload.isEmpty, payload.count <= limit else { throw SessionError.invalidPayloadSize }
            if reliability.hasMessageIndex || enqueue {
                frameCount += (payload.count + chunkSize - 1) / chunkSize
                guard frameCount <= Self.windowSize else { throw SessionError.sendWindowFull }
            }
            return chunkSize
        }
        if enqueue, queuedFrames + frameCount > Self.windowSize { throw SessionError.sendWindowFull }
        if reliability.hasMessageIndex, frameCount > 0, !canSend(frameCount) { throw SessionError.sendWindowFull }

        var prepared: [Outbound] = []
        var frames: [Frame] = []
        var size = SessionDatagram.frameHeaderSize
        for (payload, chunkSize) in zip(payloads, chunkSizes) {
            for frame in makeFrames(for: payload, reliability: reliability, channel: channel, chunkSize: chunkSize) {
                let frameSize = frame.encodedSize
                if size + frameSize > datagramLimit {
                    prepared.append(Outbound(frames: frames, size: size))
                    frames.removeAll(keepingCapacity: true)
                    size = SessionDatagram.frameHeaderSize
                }
                frames.append(frame)
                size += frameSize
            }
        }
        if !frames.isEmpty { prepared.append(Outbound(frames: frames, size: size)) }
        guard enqueue else { return prepared.map { emit($0, at: milliseconds, retries: 0) } }
        outbound[priority.rawValue].append(prepared)
        queuedFrames += frameCount
        return priority == .immediate ? flushOutbound(at: milliseconds) : []
    }

    private mutating func makeFrames(
        for payload: [UInt8], reliability: RakNetReliability, channel: UInt8, chunkSize: Int
    ) -> [Frame] {
        let lane = Int(channel)
        let order = reliability.hasOrder ? Frame.Order(index: nextOrder[lane], channel: channel) : nil
        let sequence = reliability.hasSequenceIndex ? nextSequence[lane] : nil
        if reliability.isOrdered {
            nextOrder[lane] = (nextOrder[lane] + 1) & Self.sequenceMask
            nextSequence[lane] = 0
        } else if let sequence {
            nextSequence[lane] = (sequence + 1) & Self.sequenceMask
        }

        let count = (payload.count + chunkSize - 1) / chunkSize
        let fragmentID = nextFragment
        if count > 1 { nextFragment &+= 1 }
        return (0..<count).map { index in
            let start = index * chunkSize
            let end = min(start + chunkSize, payload.count)
            let fragment = count > 1 ? Frame.Fragment(count: UInt32(count), id: fragmentID, index: UInt32(index)) : nil
            let message = reliability.hasMessageIndex ? nextMessage : nil
            if let message { nextMessage = (message + 1) & Self.sequenceMask }
            return Frame(
                reliability: reliability, messageIndex: message, sequenceIndex: sequence, order: order,
                fragment: fragment, payload: Array(payload[start..<end])
            )
        }
    }

    private func canSend(_ count: Int) -> Bool {
        let inFlight = sent.values.reduce(0) { $0 + $1.datagram.frames.count }
        guard inFlight + queuedFrames + count <= Self.windowSize else { return false }
        guard outbound.allSatisfy({ queue in
            guard let first = queue.firstReliable else { return true }
            return fitsWindow(first, adding: count)
        }) else { return false }
        return sent.values.allSatisfy { fitsWindow($0.datagram, adding: count) }
    }

    private func fitsWindow(_ datagram: Outbound, adding count: Int) -> Bool {
        guard let index = datagram.frames.first?.messageIndex else { return true }
        let age = (nextMessage &- index) & Self.sequenceMask
        return Int(age) + count <= Self.windowSize
    }

    public mutating func receive(_ datagram: [UInt8], at milliseconds: UInt64) throws -> Input {
        guard datagram.count <= peerDatagramLimit,
              let packet = SessionDatagram(datagram, sequenceLimit: Self.windowSize * 4) else { return Input() }

        switch packet {
        case let .acknowledgement(sequences):
            var sample: UInt64?
            for sequence in sequences {
                guard let packet = takePending(sequence) else { continue }
                congestion?.acknowledge(sequence: sequence, bytes: packet.datagram.size)
                guard packet.retries == 0, milliseconds >= packet.sentAt else { continue }
                let elapsed = milliseconds - packet.sentAt
                sample = min(sample ?? elapsed, elapsed) // one sample per acknowledgement
            }
            if let sample { observeRoundTripTime(sample) }
            return Input(replies: flushOutbound(at: milliseconds))
        case let .negativeAcknowledgement(sequences):
            return Input(replies: try resend(sequences, at: milliseconds))
        case let .frames(sequence, frames):
            let arrival = incoming.insert(sequence)
            guard arrival != .tooOld, arrival != .tooFar else { return Input() }
            var input = Input()
            if batchAcknowledgements {
                pendingAcknowledgements.insert(sequence)
                if pendingAcknowledgements.count >= (datagramLimit - 3) / 4,
                   let packet = flushAcknowledgements() { input.replies.append(packet) } // singleton record budget
            } else {
                input.replies.append(SessionDatagram.acknowledgement(sequence))
            }
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

    public mutating func flushAcknowledgements() -> [UInt8]? {
        let packet = SessionDatagram.acknowledgement(pendingAcknowledgements)
        pendingAcknowledgements.removeAll(keepingCapacity: true)
        return packet
    }

    public mutating func update(at milliseconds: UInt64) throws -> [[UInt8]] {
        let acknowledgement = flushAcknowledgements()
        let outbound = try retransmit(at: milliseconds)
        return acknowledgement.map { [$0] + outbound } ?? outbound
    }

    public mutating func retransmit(at milliseconds: UInt64) throws -> [[UInt8]] {
        let expired = sent.compactMap { sequence, packet in
            let delay = retransmissionTimer.delay(after: packet.retries)
            return milliseconds >= packet.sentAt && milliseconds - packet.sentAt >= delay ? sequence : nil
        }
        return try resend(expired, at: milliseconds) + flushOutbound(at: milliseconds)
    }

    var hasPending: Bool { !sent.isEmpty || queuedFrames > 0 }

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

    private mutating func emit(_ datagram: Outbound, at time: UInt64, retries: Int) -> [UInt8] {
        let sequence = takeSequence()
        if datagram.frames.first?.reliability.hasMessageIndex == true {
            sent[sequence] = Pending(datagram: datagram, sentAt: time, retries: retries)
            bytesInFlight += datagram.size
        }
        return SessionDatagram.encode(sequence: sequence, frames: datagram.frames)
    }

    private mutating func takePending(_ sequence: UInt32) -> Pending? {
        guard let packet = sent.removeValue(forKey: sequence) else { return nil }
        bytesInFlight -= packet.datagram.size
        return packet
    }

    private mutating func flushOutbound(at time: UInt64) -> [[UInt8]] {
        guard queuedFrames > 0 else { return [] }
        var available = congestion.map { $0.bytes - bytesInFlight } ?? Int.max
        var replies: [[UInt8]] = []
        for priority in outbound.indices {
            while let datagram = outbound[priority].first {
                guard datagram.size <= available else { return replies }
                replies.append(emit(datagram, at: time, retries: 0))
                queuedFrames -= datagram.frames.count
                available -= datagram.size
                outbound[priority].dropFirst()
            }
        }
        return replies
    }

    private mutating func resend(_ sequences: [UInt32], at time: UInt64) throws -> [[UInt8]] {
        guard sequences.allSatisfy({ (sent[$0]?.retries ?? 0) < 8 }) else { throw SessionError.retryLimit }
        let packets = sequences.compactMap { takePending($0) }
        if !packets.isEmpty { congestion?.reportLoss(nextSequence: nextDatagram) }
        return packets.map { emit($0.datagram, at: time, retries: $0.retries + 1) } // new datagram index
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
