public struct OnlineConnection: Sendable {
    public enum ConnectionError: Error, Equatable {
        case invalidState
    }

    public struct Input: Equatable, Sendable {
        public fileprivate(set) var replies: [[UInt8]] = []
        public fileprivate(set) var payloads: [[UInt8]] = []
        public fileprivate(set) var opened = false
        public fileprivate(set) var closed = false
    }

    private enum State: Equatable, Sendable {
        case idle(clientGUID: Int64)
        case awaitingRequest(clientGUID: Int64)
        case awaitingAcceptance(requestTime: UInt64)
        case awaitingConfirmation(serverTime: UInt64)
        case open
        case closing
        case closed
    }

    private static let localAddresses = Array(
        repeating: IPv4Endpoint(address: 0, port: 0).wireBytes, count: 20 // system address slots
    ).flatMap { $0 }

    public let peer: IPv4Endpoint
    private var state: State
    private var session: ConnectedSession

    public init(accepting client: IPv4Endpoint, negotiation: OfflineServer.Negotiation) throws {
        peer = client
        state = .awaitingRequest(clientGUID: negotiation.clientGUID)
        session = try ConnectedSession(mtu: negotiation.mtu)
    }

    public init(connectingTo server: IPv4Endpoint, mtu: UInt16, clientGUID: Int64) throws {
        peer = server
        state = .idle(clientGUID: clientGUID)
        session = try ConnectedSession(mtu: mtu)
    }

    public var isOpen: Bool { state == .open }
    public var isClosed: Bool { state == .closed }

    public mutating func start(at milliseconds: UInt64) throws -> [[UInt8]] {
        guard case let .idle(guid) = state else { throw ConnectionError.invalidState }
        let request = [0x09] + bigEndianBytes(guid) + bigEndianBytes(milliseconds) + [0]
        let packets = try session.send(request, at: milliseconds)
        state = .awaitingAcceptance(requestTime: milliseconds)
        return packets
    }

    public mutating func send(_ payload: [UInt8], at milliseconds: UInt64) throws -> [[UInt8]] {
        try send(payload, reliability: .reliableOrdered, at: milliseconds)
    }

    public mutating func send(
        _ payload: [UInt8], reliability: RakNetReliability, channel: UInt8 = 0, at milliseconds: UInt64
    ) throws -> [[UInt8]] {
        guard isOpen else { throw ConnectionError.invalidState }
        return try session.send(payload, reliability: reliability, channel: channel, at: milliseconds)
    }

    public mutating func close(at milliseconds: UInt64) throws -> [[UInt8]] {
        guard isOpen else { throw ConnectionError.invalidState }
        let packets = try session.send([0x15], at: milliseconds)
        state = .closing
        return packets
    }

    public mutating func retransmit(at milliseconds: UInt64) throws -> [[UInt8]] {
        guard !isClosed else { return [] }
        return try session.retransmit(at: milliseconds)
    }

    mutating func keepalive(at milliseconds: UInt64) -> [UInt8]? {
        guard isOpen else { return nil }
        return session.sendControl([0x00] + bigEndianBytes(milliseconds))
    }

    public mutating func receive(
        _ datagram: [UInt8], from sender: IPv4Endpoint, at milliseconds: UInt64
    ) throws -> Input {
        guard sender == peer, state != .closed else { return Input() }
        if case .idle = state { return Input() }

        let result = try session.receive(datagram, at: milliseconds)
        var input = Input(replies: result.replies)
        if state == .closing, !session.hasPending {
            state = .closed
            input.closed = true
            return input
        }
        for payload in result.payloads {
            try handle(payload, at: milliseconds, into: &input)
        }
        return input
    }

    private mutating func handle(_ packet: [UInt8], at time: UInt64, into input: inout Input) throws {
        guard let id = packet.first else { return }
        switch (state, id) {
        case let (.awaitingRequest(guid), 0x09):
            guard packet.count == 18, packet[17] == 0,
                  Int64(wireBytes: packet[1..<9]) == guid else { return }
            var reply: [UInt8] = [0x10]
            reply += peer.wireBytes
            reply += [0, 0]
            reply += Self.localAddresses
            reply += packet[9..<17]
            reply += bigEndianBytes(time)
            input.replies += try session.send(reply, at: time)
            state = .awaitingConfirmation(serverTime: time)
        case let (.awaitingAcceptance(requestTime), 0x10):
            guard let times = Self.timestamps(in: packet, includesSystemIndex: true),
                  times.echoed == requestTime else { return } // request timestamp echo
            let reply = [0x13] + peer.wireBytes + Self.localAddresses
                + bigEndianBytes(times.original) + bigEndianBytes(time)
            input.replies += try session.send(reply, at: time)
            state = .open
            input.opened = true
        case let (.awaitingConfirmation(serverTime), 0x13):
            guard let times = Self.timestamps(in: packet, includesSystemIndex: false),
                  times.echoed == serverTime else { return } // acceptance timestamp echo
            state = .open
            input.opened = true
        case (.open, 0x00):
            guard packet.count == 9 else { return }
            let pong = [0x03] + packet[1..<9] + bigEndianBytes(time)
            input.replies.append(session.sendControl(pong))
        case (.open, 0x04):
            input.replies.append(session.sendControl([0x00] + bigEndianBytes(time)))
        case (_, 0x15):
            state = .closed
            input.closed = true
        case (.open, 0x03), (.open, 0x09), (.open, 0x10), (.open, 0x13):
            break
        case (.open, _):
            input.payloads.append(packet)
        default:
            break
        }
    }

    private static func timestamps(
        in packet: [UInt8], includesSystemIndex: Bool
    ) -> (echoed: UInt64, original: UInt64)? {
        var reader = DatagramReader(packet)
        guard reader.byte() != nil, skipAddress(&reader) else { return nil }
        if includesSystemIndex, reader.read(2) == nil { return nil }
        for _ in 0..<20 {
            if reader.remaining == 16 { break }
            guard skipAddress(&reader) else { return nil }
        }
        guard let first = reader.integer(UInt64.self), let second = reader.integer(UInt64.self),
              reader.finished else { return nil }
        return (first, second)
    }

    private static func skipAddress(_ reader: inout DatagramReader) -> Bool {
        switch reader.byte() {
        case 0, 4: reader.read(6) != nil
        case 6: reader.read(28) != nil
        default: false
        }
    }
}
