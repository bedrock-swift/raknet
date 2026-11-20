import Dispatch
import Foundation

public final class RakNetListener: Sendable {
    public struct OfflinePacketLimits: Equatable, Sendable {
        public let perIP: UInt32
        public let total: UInt32

        public init(perIP: UInt32 = 120, total: UInt32 = 100_000) {
            self.perIP = perIP
            self.total = total
        }
    }

    public let localAddress: RakNetEndpoint

    private let accepted: TransportInbox<RakNetConnection>
    private let commands: TransportCommands
    private let advertisement: TransportAdvertisement
    private let access = ListenerAccess()

    private init(
        socket: UDPSocket, server: OfflineServer, maximumMTU: UInt16,
        maximumConnections: Int, maximumConnectionsPerIP: Int?, useCookies: Bool,
        offlinePacketLimits: OfflinePacketLimits?, onlinePacketsPerSecond: UInt32?
    ) {
        localAddress = socket.localAddress
        let accepted = TransportInbox<RakNetConnection>(limit: maximumConnections, byteLimit: maximumConnections)
        let commands = TransportCommands()
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
                maximumConnections: maximumConnections, maximumConnectionsPerIP: maximumConnectionsPerIP,
                useCookies: useCookies, offlinePacketLimits: offlinePacketLimits, onlinePacketsPerSecond: onlinePacketsPerSecond
            )),
            accepted: accepted, commands: commands
        )
    }

    public static func listen(
        on address: IPv4Endpoint,
        guid: UInt64 = .random(in: 1...UInt64(Int64.max)),
        advertisement: [UInt8] = [],
        maximumMTU: UInt16 = RakNetMTU.defaultValue,
        maximumConnections: Int = 128,
        maximumConnectionsPerIP: Int? = nil,
        useCookies: Bool = true,
        offlinePacketLimits: OfflinePacketLimits? = .init(),
        onlinePacketsPerSecond: UInt32? = 10_000
    ) throws -> RakNetListener {
        try listen(
            on: .ipv4(address), guid: guid, advertisement: advertisement, maximumMTU: maximumMTU,
            maximumConnections: maximumConnections, maximumConnectionsPerIP: maximumConnectionsPerIP,
            useCookies: useCookies, offlinePacketLimits: offlinePacketLimits, onlinePacketsPerSecond: onlinePacketsPerSecond
        )
    }

    public static func listen(
        on address: RakNetEndpoint,
        guid: UInt64 = .random(in: 1...UInt64(Int64.max)),
        advertisement: [UInt8] = [],
        maximumMTU: UInt16 = RakNetMTU.defaultValue,
        maximumConnections: Int = 128,
        maximumConnectionsPerIP: Int? = nil,
        useCookies: Bool = true,
        offlinePacketLimits: OfflinePacketLimits? = .init(),
        onlinePacketsPerSecond: UInt32? = 10_000
    ) throws -> RakNetListener {
        guard (1...4096).contains(maximumConnections),
              maximumConnectionsPerIP.map({ (1...4096).contains($0) }) ?? true else {
            throw RakNetTransportError.invalidConnectionLimit
        }
        let server = try OfflineServer(
            guid: guid, advertisement: advertisement, maximumMTU: maximumMTU, udpOverhead: address.udpOverhead
        )
        let socket = try UDPSocket(bindTo: address, receiveTimeout: 20)
        return RakNetListener(
            socket: socket, server: server, maximumMTU: maximumMTU,
            maximumConnections: maximumConnections, maximumConnectionsPerIP: maximumConnectionsPerIP,
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
        let maximumConnections: Int
        let maximumConnectionsPerIP: Int?
        let useCookies: Bool
        let offlinePacketLimits: RakNetListener.OfflinePacketLimits?
        let onlinePacketsPerSecond: UInt32?
    }

    enum Role: Sendable {
        case listen(Listener)
        case dial(RakNetEndpoint, mtu: UInt16, timeout: Duration)
    }

    static func launch(
        socket: UDPSocket, role: Role, accepted: TransportInbox<RakNetConnection>, commands: TransportCommands
    ) {
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
    private var connectionsByHost: [ListenerHost: Int] = [:]
    private var generation: UInt64 = 0
    private var cookies = OfflineCookies()
    private var offlineRate: OfflinePacketRate?
    private var dial: DialAttempt?

    private init(
        socket: UDPSocket, role: Role, accepted: TransportInbox<RakNetConnection>, commands: TransportCommands
    ) {
        self.socket = socket
        self.role = role
        self.accepted = accepted
        self.commands = commands
        if case let .dial(address, mtu, timeout) = role {
            dial = DialAttempt(address: address, mtu: mtu, timeout: timeout)
        } else if case let .listen(listener) = role, let limits = listener.offlinePacketLimits {
            offlineRate = OfflinePacketRate(limits: limits)
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
                if now >= nextMaintenance {
                    try maintainDial(at: now)
                    maintain(at: now)
                    nextMaintenance = now + 20
                }
                if let datagram = try socket.receive(into: &buffer), commands.isOpen {
                    try receive(datagram.bytes, from: datagram.sender, at: transportTime())
                }
                if case .dial = role, dial == nil, peers.isEmpty { break }
            }
        } catch {
            failure = error
        }
    }

    private mutating func receive(_ datagram: [UInt8], from address: RakNetEndpoint, at now: UInt64) throws {
        guard let header = datagram.first else { return }
        if case let .listen(listener) = role {
            guard !listener.access.isBlocked(address) else { return }
            if header & 0x80 == 0 {
                guard offlineRate?.allow(address, at: now) != false else { return }
                if let response = offlineResponse(datagram, from: address, listener: listener, at: now) {
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

    private mutating func offlineResponse(
        _ datagram: [UInt8], from address: RakNetEndpoint, listener: Listener, at now: UInt64
    ) -> OfflineServer.Response? {
        let server = listener.server
        switch datagram.first {
        case OfflineWire.openRequestOne where listener.useCookies:
            guard let response = server.receive(datagram, from: address) else { return nil }
            guard response.datagram.first == OfflineWire.openReplyOne else { return response }
            guard let cookie = cookies.issue(to: address, at: now) else { return nil }
            return server.receive(datagram, from: address, cookie: cookie)
        case OfflineWire.openRequestTwo where listener.useCookies:
            guard let cookie = cookies.value(for: address, at: now) else { return nil }
            return server.receive(datagram, from: address, cookie: cookie)
        default:
            return server.receive(datagram, from: address, cookie: nil) { listener.advertisement.data(for: address) }
        }
    }

    private mutating func handleOffline(
        _ response: OfflineServer.Response, from address: RakNetEndpoint, at now: UInt64, listener: Listener
    ) {
        do {
            if let negotiation = response.negotiation, peers[address] == nil {
                let connection = try OnlineConnection(
                    accepting: address, negotiation: negotiation, batchAcknowledgements: true, congestionControl: true
                )
                _ = try register(connection, mtu: negotiation.mtu, listener: listener, at: now)
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
        let host = ListenerHost(address)
        guard peers.count < listener.maximumConnections,
              listener.maximumConnectionsPerIP.map({ connectionsByHost[host, default: 0] < $0 }) ?? true else {
            throw RakNetTransportError.connectionLimitReached
        }

        let key = TransportPeerKey(address: address, generation: generation)
        generation &+= 1 // reused udp endpoints
        let peer = Peer(
            key: key, mtu: mtu, connection: connection, now: now,
            onlinePacketsPerSecond: listener.onlinePacketsPerSecond
        )
        peer.metrics.update(from: connection)
        peers[address] = peer
        if listener.maximumConnectionsPerIP != nil { connectionsByHost[host, default: 0] += 1 }
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
                if detach { remove(key.address) }
                continuation.resume(returning: snapshot)
            case let .adopt(snapshot, continuation):
                guard case let .listen(listener) = role, snapshot.localAddress == socket.localAddress else {
                    throw RakNetTransportError.snapshotAddressMismatch
                }
                let peer = try register(snapshot.restore(at: now), mtu: snapshot.mtu, listener: listener, at: now)
                for payload in snapshot.bufferedPayloads {
                    guard peer.messages.offer(payload, size: payload.count) else {
                        remove(peer.key.address)
                        throw RakNetTransportError.receiveBufferFull
                    }
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
                if now >= peer.nextPing, let ping = peer.connection.keepalive(at: now) { // idle peer probe
                    try socket.send(ping, to: address)
                    peer.nextPing = now + 2_000
                }
            } catch {
                remove(address, failure: error)
            }
        }
    }

    private mutating func remove(_ address: RakNetEndpoint, failure: (any Error)? = nil) {
        guard let peer = peers.removeValue(forKey: address) else { return }
        peer.messages.finish(failure)
        let host = ListenerHost(address)
        if let count = connectionsByHost[host] {
            connectionsByHost[host] = count > 1 ? count - 1 : nil
        }
    }

    private func send(_ packets: [[UInt8]], to address: RakNetEndpoint) throws {
        for packet in packets { try socket.send(packet, to: address) }
    }
}

func transportTime() -> UInt64 {
    DispatchTime.now().uptimeNanoseconds / 1_000_000
}
