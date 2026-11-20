import Dispatch

public final class RakNetListener: Sendable {
    public let localAddress: IPv4Endpoint

    private let accepted: TransportInbox<RakNetConnection>
    private let commands: TransportCommands

    private init(socket: UDPSocket, server: OfflineServer, maximumConnections: Int, useCookies: Bool) {
        localAddress = socket.localAddress
        let accepted = TransportInbox<RakNetConnection>(limit: maximumConnections, byteLimit: maximumConnections)
        let commands = TransportCommands()
        self.accepted = accepted
        self.commands = commands

        TransportDriver.launch(
            socket: socket, role: .listen(server, maximumConnections: maximumConnections, useCookies: useCookies),
            accepted: accepted, commands: commands
        )
    }

    public static func listen(
        on address: IPv4Endpoint,
        guid: UInt64 = .random(in: 1...UInt64(Int64.max)),
        advertisement: [UInt8] = [],
        maximumMTU: UInt16 = IPv4MTU.defaultValue,
        maximumConnections: Int = 128,
        useCookies: Bool = true
    ) throws -> RakNetListener {
        guard (1...4096).contains(maximumConnections) else { throw RakNetTransportError.invalidConnectionLimit }
        let server = try OfflineServer(guid: guid, advertisement: advertisement, maximumMTU: maximumMTU)
        let socket = try UDPSocket(bindTo: address, receiveTimeout: 20)
        return RakNetListener(socket: socket, server: server, maximumConnections: maximumConnections, useCookies: useCookies)
    }

    public func accept() async throws -> RakNetConnection? {
        try await accepted.next()
    }

    public func close() {
        commands.stop()
        accepted.finish(discardBuffered: true)
    }

    deinit {
        close()
    }
}

struct TransportDriver {
    enum Role: Sendable {
        case listen(OfflineServer, maximumConnections: Int, useCookies: Bool)
        case dial(IPv4Endpoint, mtu: UInt16, timeout: Duration)
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

        init(key: TransportPeerKey, mtu: UInt16, connection: OnlineConnection, now: UInt64) {
            self.key = key
            self.mtu = mtu
            self.connection = connection
            lastPacket = now
            nextPing = now + 2_000
        }
    }

    private let socket: UDPSocket
    private let role: Role
    private let accepted: TransportInbox<RakNetConnection>
    private let commands: TransportCommands
    private var peers: [IPv4Endpoint: Peer] = [:]
    private var generation: UInt64 = 0
    private var cookies = OfflineCookies()
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

    private mutating func receive(_ datagram: [UInt8], from address: IPv4Endpoint, at now: UInt64) throws {
        if case let .listen(server, maximumConnections, useCookies) = role,
           let response = offlineResponse(datagram, from: address, server: server, useCookies: useCookies, at: now) {
            handleOffline(response, from: address, at: now, maximumConnections: maximumConnections)
            return
        }

        guard let peer = peers[address] else {
            try receiveDialReply(datagram, from: address, at: now)
            return
        }
        guard let header = datagram.first, header & 0x80 != 0 else { return }
        do {
            let input = try peer.connection.receive(datagram, from: address, at: now)
            try send(input.replies, to: address)
            peer.lastPacket = now
            peer.metrics.update(roundTripTime: peer.connection.roundTripTime)
            if input.opened {
                dial = nil
                let connection = RakNetConnection(
                    localAddress: socket.localAddress, key: peer.key, mtu: peer.mtu,
                    messages: peer.messages, commands: commands, metrics: peer.metrics
                )
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

    private mutating func receiveDialReply(_ datagram: [UInt8], from address: IPv4Endpoint, at now: UInt64) throws {
        guard var attempt = dial, attempt.address == address else { return }
        let event = try attempt.receive(datagram, from: address, at: now)
        dial = attempt
        switch event {
        case let .send(packet):
            try socket.send(packet, to: address)
        case let .negotiated(negotiation):
            let key = TransportPeerKey(address: address, generation: generation)
            var connection = try OnlineConnection(connectingTo: address, mtu: negotiation.mtu, clientGUID: attempt.guid)
            try send(try connection.start(at: now), to: address)
            peers[address] = Peer(key: key, mtu: negotiation.mtu, connection: connection, now: now)
        case nil:
            break
        }
    }

    private mutating func offlineResponse(
        _ datagram: [UInt8], from address: IPv4Endpoint, server: OfflineServer, useCookies: Bool, at now: UInt64
    ) -> OfflineServer.Response? {
        guard useCookies else { return server.receive(datagram, from: address) }
        switch datagram.first {
        case OfflineWire.openRequestOne:
            guard let response = server.receive(datagram, from: address) else { return nil }
            guard response.datagram.first == OfflineWire.openReplyOne else { return response }
            guard let cookie = cookies.issue(to: address, at: now) else { return nil }
            return server.receive(datagram, from: address, cookie: cookie)
        case OfflineWire.openRequestTwo:
            guard let cookie = cookies.value(for: address, at: now) else { return nil }
            return server.receive(datagram, from: address, cookie: cookie)
        default:
            return server.receive(datagram, from: address)
        }
    }

    private mutating func handleOffline(
        _ response: OfflineServer.Response, from address: IPv4Endpoint, at now: UInt64, maximumConnections: Int
    ) {
        do {
            if let negotiation = response.negotiation, peers[address] == nil {
                guard peers.count < maximumConnections else { return }
                let key = TransportPeerKey(address: address, generation: generation)
                generation &+= 1 // reused udp endpoints
                let connection = try OnlineConnection(accepting: address, negotiation: negotiation)
                peers[address] = Peer(key: key, mtu: negotiation.mtu, connection: connection, now: now)
            }
            try socket.send(response.datagram, to: address)
        } catch {
            remove(address, failure: error)
        }
    }

    private mutating func perform(_ command: TransportCommands.Command, at now: UInt64) {
        do {
            switch command {
            case let .send(key, payload, reliability, channel, _):
                guard let peer = peers[key.address], peer.key == key, peer.connection.isOpen else {
                    throw RakNetTransportError.closed
                }
                let packets = try peer.connection.send(payload, reliability: reliability, channel: channel, at: now)
                try send(packets, to: key.address)
            case let .close(key, _):
                if let peer = peers[key.address], peer.key == key, peer.connection.isOpen {
                    try send(try peer.connection.close(at: now), to: key.address)
                }
            }
            command.complete(.success(()))
        } catch {
            command.complete(.failure(error))
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
                try send(try peer.connection.retransmit(at: now), to: address)
                if now >= peer.nextPing, let ping = peer.connection.keepalive(at: now) { // idle peer probe
                    try socket.send(ping, to: address)
                    peer.nextPing = now + 2_000
                }
            } catch {
                remove(address, failure: error)
            }
        }
    }

    private mutating func remove(_ address: IPv4Endpoint, failure: (any Error)? = nil) {
        peers.removeValue(forKey: address)?.messages.finish(failure)
    }

    private func send(_ packets: [[UInt8]], to address: IPv4Endpoint) throws {
        for packet in packets { try socket.send(packet, to: address) }
    }
}

func transportTime() -> UInt64 {
    DispatchTime.now().uptimeNanoseconds / 1_000_000
}
