import Foundation

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

    struct Archive: Codable {
        let sessionTime: UInt64
        let pendingPing: UInt64?
        let session: Data
    }

    private enum State: Equatable, Sendable {
        case idle(clientGUID: Int64)
        case awaitingRequest(clientGUID: Int64)
        case awaitingAcceptance(requestTime: UInt64)
        case awaitingConfirmation(clientGUID: Int64, serverTime: UInt64)
        case open
        case closing
        case closed
    }

    private static let ipv4SystemAddresses = Array(
        repeating: IPv4Endpoint(address: 0, port: 0).wireBytes, count: 20 // system address slots
    ).flatMap { $0 }

    private static let ipv6SystemAddresses = Array(
        repeating: IPv6Endpoint(addressHigh: 0, addressLow: 0, port: 0).wireBytes, count: 20
    ).flatMap { $0 }

    public let peer: RakNetEndpoint
    private var state: State
    private var session: ConnectedSession
    private var pendingPing: UInt64?
    private var clockAnchor: (external: UInt64, session: UInt64)?

    public init(
        accepting client: IPv4Endpoint, negotiation: OfflineServer.Negotiation, batchAcknowledgements: Bool = false,
        congestionControl: Bool = false
    ) throws {
        try self.init(
            accepting: .ipv4(client), negotiation: negotiation,
            batchAcknowledgements: batchAcknowledgements, congestionControl: congestionControl
        )
    }

    public init(
        accepting client: RakNetEndpoint, negotiation: OfflineServer.Negotiation, batchAcknowledgements: Bool = false,
        congestionControl: Bool = false
    ) throws {
        peer = client
        state = .awaitingRequest(clientGUID: negotiation.clientGUID)
        session = try ConnectedSession(
            mtu: negotiation.mtu, udpOverhead: client.udpOverhead,
            batchAcknowledgements: batchAcknowledgements, congestionControl: congestionControl
        )
    }

    public init(
        connectingTo server: IPv4Endpoint, mtu: UInt16, clientGUID: Int64, batchAcknowledgements: Bool = false,
        congestionControl: Bool = false
    ) throws {
        try self.init(
            connectingTo: .ipv4(server), mtu: mtu, clientGUID: clientGUID,
            batchAcknowledgements: batchAcknowledgements, congestionControl: congestionControl
        )
    }

    public init(
        connectingTo server: RakNetEndpoint, mtu: UInt16, clientGUID: Int64, batchAcknowledgements: Bool = false,
        congestionControl: Bool = false
    ) throws {
        peer = server
        state = .idle(clientGUID: clientGUID)
        session = try ConnectedSession(
            mtu: mtu, udpOverhead: server.udpOverhead,
            batchAcknowledgements: batchAcknowledgements, congestionControl: congestionControl
        )
    }

    init(restoring peer: RakNetEndpoint, mtu: UInt16, at capturedAt: UInt64, archive: Archive) throws {
        if let pendingPing = archive.pendingPing, pendingPing > archive.sessionTime {
            throw RakNetSessionSnapshot.ArchiveError.invalidState
        }
        self.peer = peer
        state = .open
        pendingPing = archive.pendingPing
        clockAnchor = capturedAt == archive.sessionTime ? nil : (capturedAt, archive.sessionTime)
        session = try ConnectedSession(
            archive: archive.session, mtu: mtu, udpOverhead: peer.udpOverhead, at: archive.sessionTime
        )
    }

    func archive(at capturedAt: UInt64) throws -> Archive {
        Archive(sessionTime: sessionTime(capturedAt), pendingPing: pendingPing, session: try session.archive())
    }

    func acceptsHandshakeRetry(clientGUID: Int64) -> Bool {
        switch state {
        case let .awaitingRequest(guid), let .awaitingConfirmation(guid, _): guid == clientGUID
        default: false
        }
    }

    public var isOpen: Bool { state == .open }
    public var isClosed: Bool { state == .closed }
    public var roundTripTime: Duration? { session.roundTripTime }
    public var sendCapacity: Int? { session.sendCapacity }

    public func maximumMessageLength(for reliability: RakNetReliability = .reliableOrdered) -> Int {
        session.maximumMessageLength(for: reliability)
    }

    public func maximumUnfragmentedMessageLength(for reliability: RakNetReliability = .reliableOrdered) -> Int {
        session.maximumUnfragmentedMessageLength(for: reliability)
    }

    public mutating func start(at milliseconds: UInt64) throws -> [[UInt8]] {
        guard case let .idle(guid) = state else { throw ConnectionError.invalidState }
        let time = sessionTime(milliseconds)
        let request = [0x09] + bigEndianBytes(guid) + bigEndianBytes(time) + [0]
        let packets = try session.send(request, at: time)
        state = .awaitingAcceptance(requestTime: time)
        return packets
    }

    public mutating func send(_ payload: [UInt8], at milliseconds: UInt64) throws -> [[UInt8]] {
        try send(payload, reliability: .reliableOrdered, at: milliseconds)
    }

    public mutating func send(
        _ payload: [UInt8], reliability: RakNetReliability, channel: UInt8 = 0,
        priority: RakNetPriority = .immediate, at milliseconds: UInt64
    ) throws -> [[UInt8]] {
        guard isOpen else { throw ConnectionError.invalidState }
        return try session.send(payload, reliability: reliability, channel: channel, priority: priority, at: sessionTime(milliseconds))
    }

    public mutating func sendBatch(
        _ payloads: [[UInt8]], reliability: RakNetReliability = .reliableOrdered, channel: UInt8 = 0,
        priority: RakNetPriority = .immediate, at milliseconds: UInt64
    ) throws -> [[UInt8]] {
        guard isOpen else { throw ConnectionError.invalidState }
        return try session.sendBatch(
            payloads, reliability: reliability, channel: channel, priority: priority, at: sessionTime(milliseconds)
        )
    }

    mutating func abort() -> [UInt8]? {
        guard state == .open || state == .closing else { return nil }
        let packet = session.sendControl([0x15])
        state = .closed
        return packet
    }

    public mutating func close(at milliseconds: UInt64) throws -> [[UInt8]] {
        guard isOpen else { throw ConnectionError.invalidState }
        let packets = try session.send([0x15], at: sessionTime(milliseconds))
        state = .closing
        return packets
    }

    public mutating func flushAcknowledgements() -> [UInt8]? {
        session.flushAcknowledgements()
    }

    public mutating func update(at milliseconds: UInt64) throws -> [[UInt8]] {
        guard !isClosed else { return [] }
        return try session.update(at: sessionTime(milliseconds))
    }

    public mutating func retransmit(at milliseconds: UInt64) throws -> [[UInt8]] {
        guard !isClosed else { return [] }
        return try session.retransmit(at: sessionTime(milliseconds))
    }

    public mutating func keepalive(at milliseconds: UInt64) -> [UInt8]? {
        guard isOpen else { return nil }
        return makeKeepalive(at: sessionTime(milliseconds))
    }

    private mutating func makeKeepalive(at time: UInt64) -> [UInt8] {
        pendingPing = time
        return session.sendControl([0x00] + bigEndianBytes(time))
    }

    public mutating func receive(
        _ datagram: [UInt8], from sender: IPv4Endpoint, at milliseconds: UInt64
    ) throws -> Input {
        try receive(datagram, from: .ipv4(sender), at: milliseconds)
    }

    public mutating func receive(
        _ datagram: [UInt8], from sender: RakNetEndpoint, at milliseconds: UInt64
    ) throws -> Input {
        guard sender == peer, state != .closed else { return Input() }
        if case .idle = state { return Input() }

        let time = sessionTime(milliseconds)
        let result = try session.receive(datagram, at: time)
        var input = Input(replies: result.replies)
        if state == .closing, !session.hasPending {
            state = .closed
            input.closed = true
            if let ack = flushAcknowledgements() { input.replies.append(ack) }
            return input
        }
        for payload in result.payloads {
            try handle(payload, at: time, into: &input)
        }
        if input.closed, let ack = flushAcknowledgements() { input.replies.append(ack) }
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
            reply += localAddresses
            reply += packet[9..<17]
            reply += bigEndianBytes(time)
            input.replies += try session.send(reply, at: time)
            state = .awaitingConfirmation(clientGUID: guid, serverTime: time)
        case let (.awaitingAcceptance(requestTime), 0x10):
            guard let times = Self.timestamps(in: packet, includesSystemIndex: true),
                  times.echoed == requestTime else { return } // request timestamp echo
            let reply = [0x13] + peer.wireBytes + localAddresses
                + bigEndianBytes(times.original) + bigEndianBytes(time)
            input.replies += try session.send(reply, at: time)
            state = .open
            input.opened = true
        case let (.awaitingConfirmation(_, serverTime), 0x13):
            guard let times = Self.timestamps(in: packet, includesSystemIndex: false),
                  times.echoed == serverTime else { return } // acceptance timestamp echo
            state = .open
            input.opened = true
        case (.open, 0x00):
            guard packet.count == 9 else { return }
            let pong = [0x03] + packet[1..<9] + bigEndianBytes(time)
            input.replies.append(session.sendControl(pong))
        case (.open, 0x04):
            input.replies.append(makeKeepalive(at: time))
        case (.open, 0x03):
            guard packet.count == 17, let pendingPing, time >= pendingPing,
                  UInt64(wireBytes: packet[1..<9]) == pendingPing else { return } // outstanding ping echo
            self.pendingPing = nil
            session.observeRoundTripTime(time - pendingPing)
        case (_, 0x15):
            state = .closed
            input.closed = true
        case (.open, 0x09), (.open, 0x10), (.open, 0x13):
            break
        case (.open, _):
            input.payloads.append(packet)
        default:
            break
        }
    }

    mutating func rebaseClock(from capturedAt: UInt64, to milliseconds: UInt64) {
        clockAnchor = (milliseconds, sessionTime(capturedAt))
    }

    private func sessionTime(_ milliseconds: UInt64) -> UInt64 {
        guard let anchor = clockAnchor else { return milliseconds }
        if milliseconds >= anchor.external {
            return anchor.session + min(milliseconds - anchor.external, UInt64.max - anchor.session)
        }
        return anchor.session - min(anchor.external - milliseconds, anchor.session)
    }

    private var localAddresses: [UInt8] {
        switch peer {
        case .ipv4: Self.ipv4SystemAddresses
        case .ipv6: Self.ipv6SystemAddresses
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
