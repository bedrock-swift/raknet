public struct RakNetServer: Sendable {
    public struct Datagram: Equatable, Sendable {
        public let destination: RakNetEndpoint
        public let bytes: [UInt8]
    }

    public enum Event: Equatable, Sendable {
        case connected(RakNetEndpoint, mtu: UInt16)
        case payload(RakNetEndpoint, [UInt8])
        case disconnected(RakNetEndpoint)
    }

    public struct Output: Equatable, Sendable {
        public fileprivate(set) var datagrams: [Datagram] = []
        public fileprivate(set) var events: [Event] = []
    }

    private struct Peer: Sendable {
        let mtu: UInt16
        var connection: OnlineConnection
        var established: Bool
        var lastPacket: UInt64
        var nextPing: UInt64
        var onlineRate: OnlinePacketRate?
    }

    public let localAddress: RakNetEndpoint
    private let onlinePacketsPerSecond: UInt32?
    private var quota: ConnectionAdmission
    private var offlineRate: OfflinePacketRate?
    private var temporaryBlocks: [ListenerHost: UInt64] = [:]
    private var handshake: OfflineServerHandshake
    private var peers: [RakNetEndpoint: Peer] = [:]

    public init(
        on address: RakNetEndpoint, server: OfflineServer, maximumConnections: Int = 128,
        maximumConnectionsPerIP: Int? = nil, pendingConnections: RakNetPendingConnectionLimits = .init(),
        useCookies: Bool = true,
        offlinePacketLimits: RakNetOfflinePacketLimits? = .init(), onlinePacketsPerSecond: UInt32? = 10_000
    ) throws {
        localAddress = address
        quota = try ConnectionAdmission(maximum: maximumConnections, perIP: maximumConnectionsPerIP, pending: pendingConnections)
        offlineRate = offlinePacketLimits.map { OfflinePacketRate(limits: $0) }
        self.onlinePacketsPerSecond = onlinePacketsPerSecond
        handshake = OfflineServerHandshake(server: server, useCookies: useCookies)
    }

    public mutating func block(_ address: RakNetEndpoint, for milliseconds: UInt64 = 10_000, at time: UInt64) {
        guard milliseconds > 0 else { return }
        let host = ListenerHost(address)
        let deadline = time + min(milliseconds, UInt64.max - time)
        temporaryBlocks[host] = max(temporaryBlocks[host, default: deadline], deadline)
    }

    public mutating func unblock(_ address: RakNetEndpoint) {
        temporaryBlocks.removeValue(forKey: ListenerHost(address))
    }

    private mutating func isBlocked(_ address: RakNetEndpoint, at time: UInt64) -> Bool {
        guard !temporaryBlocks.isEmpty else { return false }
        let host = ListenerHost(address)
        guard let deadline = temporaryBlocks[host] else { return false }
        if time < deadline { return true }
        temporaryBlocks.removeValue(forKey: host)
        return false
    }

    public mutating func receive(_ datagram: [UInt8], from address: RakNetEndpoint, at time: UInt64) -> Output {
        guard let header = datagram.first, !isBlocked(address, at: time) else { return Output() }
        if header & 0x80 == 0 {
            guard offlineRate?.allow(address, at: time) != false else { return Output() }
            return receiveOffline(datagram, from: address, at: time)
        }
        guard var peer = peers.removeValue(forKey: address) else { return Output() }
        guard peer.onlineRate?.allow(at: time) != false else {
            peers[address] = peer
            return Output()
        }

        do {
            let input = try peer.connection.receive(datagram, from: address, at: time)
            if input.opened {
                try quota.promote(address)
                peer.established = true
            }
            peer.lastPacket = time
            var output = Output(datagrams: Self.datagrams(input.replies, to: address))
            if input.opened { output.events.append(.connected(address, mtu: peer.mtu)) }
            output.events += input.payloads.map { .payload(address, $0) }
            if input.closed {
                quota.release(address, established: peer.established)
                output.events.append(.disconnected(address))
            } else {
                peers[address] = peer
            }
            return output
        } catch {
            quota.release(address, established: peer.established)
            let packet = peer.connection.abort().map { Datagram(destination: address, bytes: $0) }
            return Output(datagrams: packet.map { [$0] } ?? [], events: [.disconnected(address)])
        }
    }

    public mutating func update(at time: UInt64) -> Output {
        if !temporaryBlocks.isEmpty { temporaryBlocks = temporaryBlocks.filter { time < $0.value } }
        var output = Output()
        for address in Array(peers.keys) {
            guard var peer = peers.removeValue(forKey: address) else { continue }
            let timeout: UInt64 = peer.connection.isOpen ? 15_000 : 5_000
            if time >= peer.lastPacket, time - peer.lastPacket >= timeout {
                quota.release(address, established: peer.established)
                output.events.append(.disconnected(address))
                continue
            }

            do {
                output.datagrams += Self.datagrams(try peer.connection.update(at: time), to: address)
                if time >= peer.nextPing, time >= peer.lastPacket, time - peer.lastPacket >= 2_000,
                   let ping = peer.connection.keepalive(at: time) {
                    output.datagrams.append(Datagram(destination: address, bytes: ping))
                    peer.nextPing = time + min(2_000, UInt64.max - time)
                }
                peers[address] = peer
            } catch {
                quota.release(address, established: peer.established)
                if let packet = peer.connection.abort() {
                    output.datagrams.append(Datagram(destination: address, bytes: packet))
                }
                output.events.append(.disconnected(address))
            }
        }
        return output
    }

    public mutating func send(
        _ payload: [UInt8], to address: RakNetEndpoint, reliability: RakNetReliability = .reliableOrdered,
        channel: UInt8 = 0, priority: RakNetPriority = .immediate, at time: UInt64
    ) throws -> [Datagram] {
        try sendBatch([payload], to: address, reliability: reliability, channel: channel, priority: priority, at: time)
    }

    public mutating func sendBatch(
        _ payloads: [[UInt8]], to address: RakNetEndpoint, reliability: RakNetReliability = .reliableOrdered,
        channel: UInt8 = 0, priority: RakNetPriority = .immediate, at time: UInt64
    ) throws -> [Datagram] {
        guard peers[address]?.connection.isOpen == true,
              let packets = try peers[address]?.connection.sendBatch(
                  payloads, reliability: reliability, channel: channel, priority: priority, at: time
              ) else { throw RakNetTransportError.closed }
        return Self.datagrams(packets, to: address)
    }

    public mutating func close(_ address: RakNetEndpoint, at time: UInt64) throws -> [Datagram] {
        let packets = try peers[address]?.connection.close(at: time) ?? []
        return Self.datagrams(packets, to: address)
    }

    public func snapshot(_ address: RakNetEndpoint, at time: UInt64) -> RakNetSessionSnapshot? {
        guard let peer = peers[address], peer.connection.isOpen else { return nil }
        return RakNetSessionSnapshot(
            localAddress: localAddress, mtu: peer.mtu, state: peer.connection, bufferedPayloads: [], capturedAt: time
        )
    }

    public mutating func adopt(_ snapshot: RakNetSessionSnapshot, at time: UInt64) throws -> Output {
        guard snapshot.localAddress == localAddress else { throw RakNetTransportError.snapshotAddressMismatch }
        let address = snapshot.remoteAddress
        guard peers[address] == nil else { throw RakNetTransportError.connectionAlreadyExists }
        try install(snapshot.restore(at: time), mtu: snapshot.mtu, at: time)
        return Output(events: [.connected(address, mtu: snapshot.mtu)] + snapshot.bufferedPayloads.map {
            .payload(address, $0)
        })
    }

    private mutating func receiveOffline(_ datagram: [UInt8], from address: RakNetEndpoint, at time: UInt64) -> Output {
        guard let response = handshake.receive(datagram, from: address, at: time) else { return Output() }
        if let negotiation = response.negotiation {
            if let existing = peers[address] {
                guard existing.mtu == negotiation.mtu,
                      existing.connection.acceptsHandshakeRetry(clientGUID: negotiation.clientGUID) else { return Output() }
            } else {
                do {
                    let connection = try OnlineConnection(
                        accepting: address, negotiation: negotiation, batchAcknowledgements: true, congestionControl: true
                    )
                    try install(connection, mtu: negotiation.mtu, at: time)
                } catch {
                    return Output()
                }
            }
        }
        return Output(datagrams: [Datagram(destination: address, bytes: response.datagram)])
    }

    private mutating func install(_ connection: OnlineConnection, mtu: UInt16, at time: UInt64) throws {
        try quota.reserve(connection.peer, established: connection.isOpen)
        peers[connection.peer] = Peer(
            mtu: mtu, connection: connection, established: connection.isOpen, lastPacket: time, nextPing: time + min(2_000, UInt64.max - time),
            onlineRate: onlinePacketsPerSecond.map { OnlinePacketRate(packetsPerSecond: $0, at: time) }
        )
    }

    private static func datagrams(_ packets: [[UInt8]], to destination: RakNetEndpoint) -> [Datagram] {
        packets.map { Datagram(destination: destination, bytes: $0) }
    }
}
