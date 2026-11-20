import Dispatch
import Foundation

public final class RakNetListener: Sendable {
    public typealias OfflinePacketLimits = RakNetOfflinePacketLimits

    public let localAddress: RakNetEndpoint

    private let accepted: TransportInbox<RakNetConnection>
    private let commands: TransportCommands
    private let advertisement: TransportAdvertisement
    private let access = ListenerAccess()

    private init(
        socket: UDPSocket, server: OfflineServer, maximumMTU: UInt16,
        quota: ConnectionAdmission, useCookies: Bool,
        offlinePacketLimits: OfflinePacketLimits?, onlinePacketsPerSecond: UInt32?
    ) {
        localAddress = socket.localAddress
        let accepted = TransportInbox<RakNetConnection>(limit: quota.maximum, byteLimit: quota.maximum)
        let commands = TransportCommands(peerLimit: quota.maximum)
        self.accepted = accepted
        self.commands = commands
        let advertisement = TransportAdvertisement(
            encoded: server.advertisement, maximumMTU: maximumMTU, udpOverhead: socket.localAddress.udpOverhead
        )
        self.advertisement = advertisement

        TransportDriver.launch(
            socket: socket,
            role: .listen(.init(
                server: server, advertisement: advertisement, access: access,
                quota: quota, useCookies: useCookies, offlinePacketLimits: offlinePacketLimits, onlinePacketsPerSecond: onlinePacketsPerSecond
            )),
            accepted: accepted, commands: commands
        )
    }

    public static func listen(
        on address: IPv4Endpoint,
        guid: UInt64 = .random(in: 1...UInt64(Int64.max)),
        advertisement: [UInt8] = [],
        supportedProtocols: [UInt8] = [11],
        maximumMTU: UInt16 = RakNetMTU.defaultValue,
        maximumConnections: Int = 128,
        maximumConnectionsPerIP: Int? = nil,
        pendingConnections: RakNetPendingConnectionLimits = .init(),
        useCookies: Bool = true,
        offlinePacketLimits: OfflinePacketLimits? = .init(),
        onlinePacketsPerSecond: UInt32? = 10_000
    ) throws -> RakNetListener {
        try listen(
            on: .ipv4(address), guid: guid, advertisement: advertisement, supportedProtocols: supportedProtocols,
            maximumMTU: maximumMTU,
            maximumConnections: maximumConnections, maximumConnectionsPerIP: maximumConnectionsPerIP,
            pendingConnections: pendingConnections, useCookies: useCookies, offlinePacketLimits: offlinePacketLimits, onlinePacketsPerSecond: onlinePacketsPerSecond
        )
    }

    public static func listen(
        on address: RakNetEndpoint,
        guid: UInt64 = .random(in: 1...UInt64(Int64.max)),
        advertisement: [UInt8] = [],
        supportedProtocols: [UInt8] = [11],
        maximumMTU: UInt16 = RakNetMTU.defaultValue,
        maximumConnections: Int = 128,
        maximumConnectionsPerIP: Int? = nil,
        pendingConnections: RakNetPendingConnectionLimits = .init(),
        useCookies: Bool = true,
        offlinePacketLimits: OfflinePacketLimits? = .init(),
        onlinePacketsPerSecond: UInt32? = 10_000
    ) throws -> RakNetListener {
        let quota = try ConnectionAdmission(maximum: maximumConnections, perIP: maximumConnectionsPerIP, pending: pendingConnections)
        let server = try OfflineServer(
            guid: guid, advertisement: advertisement, supportedProtocols: supportedProtocols,
            maximumMTU: maximumMTU, udpOverhead: address.udpOverhead
        )
        let socket = try UDPSocket(bindTo: address, receiveTimeout: 20)
        return RakNetListener(
            socket: socket, server: server, maximumMTU: maximumMTU, quota: quota,
            useCookies: useCookies, offlinePacketLimits: offlinePacketLimits, onlinePacketsPerSecond: onlinePacketsPerSecond
        )
    }

    public func setAdvertisement(_ data: [UInt8]) throws {
        try advertisement.set(data)
    }

    public func setAdvertisementProvider(_ provider: (@Sendable (RakNetEndpoint) -> [UInt8])?) {
        advertisement.setProvider(provider)
    }

    public func block(_ endpoint: RakNetEndpoint, for duration: Duration = .seconds(10)) {
        access.block(endpoint, for: duration)
    }

    public func unblock(_ endpoint: RakNetEndpoint) {
        access.unblock(endpoint)
    }

    public func accept() async throws -> RakNetConnection? {
        try await accepted.next()
    }

    public func adopt(_ snapshot: RakNetSessionSnapshot) async throws -> RakNetConnection {
        try Task.checkCancellation()
        return try await withCheckedThrowingContinuation { continuation in
            commands.submit(.adopt(snapshot, continuation))
        }
    }

    public func close() {
        commands.stop()
        accepted.finish(discardBuffered: true)
    }

    deinit {
        close()
    }
}

final class TransportAdvertisement: @unchecked Sendable {
    private let lock = NSLock()
    private let maximumMTU: UInt16
    private let udpOverhead: Int
    private var encoded: [UInt8]
    private var provider: (@Sendable (RakNetEndpoint) -> [UInt8])?

    init(encoded: [UInt8], maximumMTU: UInt16, udpOverhead: Int) {
        self.encoded = encoded
        self.maximumMTU = maximumMTU
        self.udpOverhead = udpOverhead
    }

    func set(_ data: [UInt8]) throws {
        let value = try OfflineServer.encodeAdvertisement(data, maximumMTU: maximumMTU, udpOverhead: udpOverhead)
        lock.withLock { encoded = value }
    }

    func setProvider(_ provider: (@Sendable (RakNetEndpoint) -> [UInt8])?) {
        lock.withLock { self.provider = provider }
    }

    func data(for sender: RakNetEndpoint) -> [UInt8] {
        let (provider, fallback) = lock.withLock { (provider, encoded) }
        guard let provider else { return fallback }
        let data = provider(sender)
        return (try? OfflineServer.encodeAdvertisement(data, maximumMTU: maximumMTU, udpOverhead: udpOverhead)) ?? fallback
    }
}

struct TransportDriver {
    struct Listener: Sendable {
        let server: OfflineServer
        let advertisement: TransportAdvertisement
        let access: ListenerAccess
        let quota: ConnectionAdmission
        let useCookies: Bool
        let offlinePacketLimits: RakNetListener.OfflinePacketLimits?
        let onlinePacketsPerSecond: UInt32?
    }

    enum Role: Sendable {
        case listen(Listener)
        case dial(RakNetEndpoint, mtu: UInt16, timeout: Duration, protocolVersion: UInt8)
        case resume

        var isClient: Bool {
            if case .listen = self { return false }
            return true
        }
    }

    static func launch(
        socket: UDPSocket, role: Role, accepted: TransportInbox<RakNetConnection>, commands: TransportCommands
    ) {
        if let wake = try? UDPSocket(clientFor: socket.localAddress) {
            let destination = socket.localAddress.forLocalWake
            commands.installWake { try? wake.send([0], to: destination) }
        }
        DispatchQueue(label: "raknet.udp").async { // socket owner
            var driver = TransportDriver(socket: socket, role: role, accepted: accepted, commands: commands)
            driver.run()
        }
    }

    private final class Peer {
        let key: TransportPeerKey
        let mtu: UInt16
        let messages = TransportInbox<[UInt8]>(limit: 64, byteLimit: 512 * 1024)
        let metrics = TransportMetrics()
        var connection: OnlineConnection
        var established: Bool
        var lastPacket: UInt64
        var nextPing: UInt64
        var onlineRate: OnlinePacketRate?

        init(
            key: TransportPeerKey, mtu: UInt16, connection: OnlineConnection, now: UInt64,
            onlinePacketsPerSecond: UInt32? = nil
        ) {
            self.key = key
            self.mtu = mtu
            self.connection = connection
            established = connection.isOpen
            lastPacket = now
            nextPing = now + 2_000
            onlineRate = onlinePacketsPerSecond.map { OnlinePacketRate(packetsPerSecond: $0, at: now) }
        }
    }

    private let socket: UDPSocket
    private let role: Role
    private let accepted: TransportInbox<RakNetConnection>
    private let commands: TransportCommands
    private var peers: [RakNetEndpoint: Peer] = [:]
    private var quota: ConnectionAdmission?
    private var generation: UInt64 = 0
    private var handshake: OfflineServerHandshake?
    private var offlineRate: OfflinePacketRate?
    private var dial: DialAttempt?

    private init(
        socket: UDPSocket, role: Role, accepted: TransportInbox<RakNetConnection>, commands: TransportCommands
    ) {
        self.socket = socket
        self.role = role
        self.accepted = accepted
        self.commands = commands
        if case let .dial(address, mtu, timeout, protocolVersion) = role {
            dial = DialAttempt(address: address, mtu: mtu, timeout: timeout, protocolVersion: protocolVersion)
        } else if case let .listen(listener) = role {
            handshake = OfflineServerHandshake(server: listener.server, useCookies: listener.useCookies)
            quota = listener.quota
            offlineRate = listener.offlinePacketLimits.map { OfflinePacketRate(limits: $0) }
        }
    }

    private mutating func run() {
        var failure: (any Error)?
        var buffer = [UInt8](repeating: 0, count: 65_507)
        var nextMaintenance: UInt64 = 0

        defer {
            commands.stop()
            accepted.finish(failure, discardBuffered: true)
            for peer in peers.values { peer.messages.finish(failure) }
        }

        do {
            while commands.isOpen {
                let now = transportTime()
                for command in commands.drain() { perform(command, at: now) }
                guard commands.isOpen else { break }
                if now >= nextMaintenance {
                    try maintainDial(at: now)
                    maintain(at: now)
                    nextMaintenance = now + 20
                }
                if let datagram = try socket.receive(into: &buffer), commands.isOpen,
                   datagram.bytes != [0] { // command wakeup
                    try receive(datagram.bytes, from: datagram.sender, at: transportTime())
                }
                if role.isClient, dial == nil, peers.isEmpty { break }
            }
        } catch {
            failure = error
        }
    }

    private mutating func startResumedClient(_ snapshot: RakNetSessionSnapshot, at now: UInt64) throws -> RakNetConnection {
        guard peers.isEmpty else { throw RakNetTransportError.connectionAlreadyExists }
        let key = TransportPeerKey(address: snapshot.remoteAddress, generation: generation)
        let connection = snapshot.restore(at: now)
        let peer = Peer(key: key, mtu: snapshot.mtu, connection: connection, now: now)
        peer.metrics.update(from: connection)
        try restoreBufferedPayloads(snapshot, to: peer)
        peers[key.address] = peer
        return connectionHandle(for: peer)
    }

    private func restoreBufferedPayloads(_ snapshot: RakNetSessionSnapshot, to peer: Peer) throws {
        for payload in snapshot.bufferedPayloads {
            guard peer.messages.offer(payload, size: payload.count) else {
                throw RakNetTransportError.receiveBufferFull
            }
        }
    }

    private mutating func receive(_ datagram: [UInt8], from address: RakNetEndpoint, at now: UInt64) throws {
        guard let header = datagram.first else { return }
        if case let .listen(listener) = role {
            guard !listener.access.isBlocked(address) else { return }
            if header & 0x80 == 0 {
                guard offlineRate?.allow(address, at: now) != false else { return }
                if let response = handshake?.receive(datagram, from: address, at: now, advertisement: {
                    listener.advertisement.data(for: address)
                }) {
                    handleOffline(response, from: address, at: now, listener: listener)
                }
                return
            }
        }

        guard let peer = peers[address] else {
            try receiveDialReply(datagram, from: address, at: now)
            return
        }
        guard header & 0x80 != 0, peer.onlineRate?.allow(at: now) != false else { return }
        do {
            let input = try peer.connection.receive(datagram, from: address, at: now)
            if input.opened {
                try quota?.promote(address)
                peer.established = true
            }
            try send(input.replies, to: address)
            peer.lastPacket = now
            peer.metrics.update(from: peer.connection)
            if input.opened {
                dial = nil
                let connection = connectionHandle(for: peer)
                guard accepted.offer(connection) else {
                    try send(try peer.connection.close(at: now), to: address)
                    remove(address)
                    return
                }
            }
            for payload in input.payloads {
                guard peer.messages.offer(payload, size: payload.count) else {
                    try send(try peer.connection.close(at: now), to: address)
                    remove(address, failure: RakNetTransportError.receiveBufferFull)
                    return
                }
            }
            if input.closed {
                remove(address)
                if dial != nil { throw RakNetTransportError.closed }
            }
        } catch {
            if let packet = peer.connection.abort() { try? socket.send(packet, to: address) }
            remove(address, failure: error)
            if dial != nil { throw error }
        }
    }

    private mutating func maintainDial(at now: UInt64) throws {
        guard var attempt = dial else { return }
        guard !attempt.expired else { throw RakNetTransportError.timedOut }
        if peers[attempt.address] == nil, let packet = try attempt.scheduledPacket(at: now) {
            try socket.send(packet, to: attempt.address)
        }
        dial = attempt
    }

    private mutating func receiveDialReply(_ datagram: [UInt8], from address: RakNetEndpoint, at now: UInt64) throws {
        guard var attempt = dial, attempt.address == address else { return }
        let event = try attempt.receive(datagram, from: address, at: now)
        dial = attempt
        switch event {
        case let .send(packet):
            try socket.send(packet, to: address)
        case let .negotiated(negotiation):
            let key = TransportPeerKey(address: address, generation: generation)
            var connection = try OnlineConnection(
                connectingTo: address, mtu: negotiation.mtu, clientGUID: attempt.guid,
                batchAcknowledgements: true, congestionControl: true
            )
            try send(try connection.start(at: now), to: address)
            peers[address] = Peer(key: key, mtu: negotiation.mtu, connection: connection, now: now)
        case nil:
            break
        }
    }

    private mutating func handleOffline(
        _ response: OfflineServer.Response, from address: RakNetEndpoint, at now: UInt64, listener: Listener
    ) {
        do {
            if let negotiation = response.negotiation {
                if let existing = peers[address] {
                    guard existing.mtu == negotiation.mtu,
                          existing.connection.acceptsHandshakeRetry(clientGUID: negotiation.clientGUID) else { return }
                } else {
                    let connection = try OnlineConnection(
                        accepting: address, negotiation: negotiation, batchAcknowledgements: true, congestionControl: true
                    )
                    _ = try register(connection, mtu: negotiation.mtu, listener: listener, at: now)
                }
            }
            try socket.send(response.datagram, to: address)
        } catch {
            remove(address, failure: error)
        }
    }

    private mutating func register(
        _ connection: OnlineConnection, mtu: UInt16, listener: Listener, at now: UInt64
    ) throws -> Peer {
        let address = connection.peer
        guard peers[address] == nil else { throw RakNetTransportError.connectionAlreadyExists }
        try quota?.reserve(address, established: connection.isOpen)

        let key = TransportPeerKey(address: address, generation: generation)
        generation &+= 1 // reused udp endpoints
        let peer = Peer(
            key: key, mtu: mtu, connection: connection, now: now,
            onlinePacketsPerSecond: listener.onlinePacketsPerSecond
        )
        peer.metrics.update(from: connection)
        peers[address] = peer
        return peer
    }

    private func connectionHandle(for peer: Peer) -> RakNetConnection {
        RakNetConnection(
            localAddress: socket.localAddress, key: peer.key, mtu: peer.mtu,
            messages: peer.messages, commands: commands, metrics: peer.metrics
        )
    }

    private mutating func perform(_ command: TransportCommands.Command, at now: UInt64) {
        do {
            switch command {
            case let .send(key, payloads, reliability, channel, priority, continuation):
                guard let peer = peers[key.address], peer.key == key, peer.connection.isOpen else {
                    throw RakNetTransportError.closed
                }
                let packets = try peer.connection.sendBatch(
                    payloads, reliability: reliability, channel: channel, priority: priority, at: now
                )
                peer.metrics.update(from: peer.connection)
                try send(packets, to: key.address)
                continuation.resume()
            case let .close(key, continuation):
                if let peer = peers[key.address], peer.key == key, peer.connection.isOpen {
                    try send(try peer.connection.close(at: now), to: key.address)
                }
                continuation?.resume()
            case let .snapshot(key, detach, continuation):
                guard let peer = peers[key.address], peer.key == key, peer.connection.isOpen else {
                    throw RakNetTransportError.closed
                }
                let buffered = detach ? peer.messages.drainAndFinish() : peer.messages.bufferedValues()
                let snapshot = RakNetSessionSnapshot(
                    localAddress: socket.localAddress, mtu: peer.mtu, state: peer.connection,
                    bufferedPayloads: buffered, capturedAt: now
                )
                if detach {
                    remove(key.address)
                    if role.isClient {
                        socket.close()
                        commands.stop()
                    }
                }
                continuation.resume(returning: snapshot)
            case let .adopt(snapshot, continuation):
                guard snapshot.localAddress == socket.localAddress else { throw RakNetTransportError.snapshotAddressMismatch }
                if case .resume = role {
                    continuation.resume(returning: try startResumedClient(snapshot, at: now))
                    return
                }
                guard case let .listen(listener) = role else { throw RakNetTransportError.closed }
                let peer = try register(snapshot.restore(at: now), mtu: snapshot.mtu, listener: listener, at: now)
                do {
                    try restoreBufferedPayloads(snapshot, to: peer)
                } catch {
                    remove(peer.key.address)
                    throw error
                }
                continuation.resume(returning: connectionHandle(for: peer))
            }
        } catch {
            command.fail(error)
        }
    }

    private mutating func maintain(at now: UInt64) {
        for (address, peer) in Array(peers) {
            let timeout: UInt64 = peer.connection.isOpen ? 15_000 : 5_000
            guard now - peer.lastPacket < timeout else {
                remove(address, failure: RakNetTransportError.timedOut)
                continue
            }
            do {
                let packets = try peer.connection.update(at: now)
                if !packets.isEmpty { peer.metrics.update(from: peer.connection) }
                try send(packets, to: address)
                if now >= peer.nextPing, now - peer.lastPacket >= 2_000,
                   let ping = peer.connection.keepalive(at: now) { // idle peer probe
                    try socket.send(ping, to: address)
                    peer.nextPing = now + 2_000
                }
            } catch {
                if let packet = peer.connection.abort() { try? socket.send(packet, to: address) }
                remove(address, failure: error)
            }
        }
    }

    private mutating func remove(_ address: RakNetEndpoint, failure: (any Error)? = nil) {
        guard let peer = peers.removeValue(forKey: address) else { return }
        peer.messages.finish(failure)
        quota?.release(address, established: peer.established)
    }

    private func send(_ packets: [[UInt8]], to address: RakNetEndpoint) throws {
        for packet in packets { try socket.send(packet, to: address) }
    }
}

func transportTime() -> UInt64 {
    DispatchTime.now().uptimeNanoseconds / 1_000_000
}
