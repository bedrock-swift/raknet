import Dispatch

public final class RakNetListener: Sendable {
    public let localAddress: IPv4Endpoint

    private let accepted: TransportInbox<RakNetConnection>
    private let commands: TransportCommands

    private init(socket: UDPSocket, server: OfflineServer, maximumConnections: Int) {
        localAddress = socket.localAddress
        let accepted = TransportInbox<RakNetConnection>(limit: maximumConnections, byteLimit: maximumConnections)
        let commands = TransportCommands()
        self.accepted = accepted
        self.commands = commands

        DispatchQueue(label: "raknet.listener").async { // socket owner
            var driver = ListenerDriver(
                socket: socket, server: server, maximumConnections: maximumConnections,
                accepted: accepted, commands: commands
            )
            driver.run()
        }
    }

    public static func listen(
        on address: IPv4Endpoint,
        guid: UInt64 = .random(in: 1...UInt64(Int64.max)),
        advertisement: [UInt8] = [],
        maximumMTU: UInt16 = IPv4MTU.defaultValue,
        maximumConnections: Int = 128
    ) throws -> RakNetListener {
        guard (1...4096).contains(maximumConnections) else { throw RakNetTransportError.invalidConnectionLimit }
        let server = try OfflineServer(guid: guid, advertisement: advertisement, maximumMTU: maximumMTU)
        let socket = try UDPSocket(bindTo: address, receiveTimeout: 20)
        return RakNetListener(socket: socket, server: server, maximumConnections: maximumConnections)
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

private struct ListenerDriver {
    private final class Peer {
        let key: TransportPeerKey
        let mtu: UInt16
        let messages = TransportInbox<[UInt8]>(limit: 64, byteLimit: 512 * 1024)
        var connection: OnlineConnection
        var lastPacket: UInt64
        var nextPing: UInt64

        init(key: TransportPeerKey, negotiation: OfflineServer.Negotiation, now: UInt64) throws {
            self.key = key
            mtu = negotiation.mtu
            connection = try OnlineConnection(accepting: key.address, negotiation: negotiation)
            lastPacket = now
            nextPing = now + 2_000
        }
    }

    let socket: UDPSocket
    let server: OfflineServer
    let maximumConnections: Int
    let accepted: TransportInbox<RakNetConnection>
    let commands: TransportCommands
    private var peers: [IPv4Endpoint: Peer] = [:]
    private var generation: UInt64 = 0

    init(
        socket: UDPSocket, server: OfflineServer, maximumConnections: Int,
        accepted: TransportInbox<RakNetConnection>, commands: TransportCommands
    ) {
        self.socket = socket
        self.server = server
        self.maximumConnections = maximumConnections
        self.accepted = accepted
        self.commands = commands
    }

    mutating func run() {
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
                    maintain(at: now)
                    nextMaintenance = now + 20
                }
                if let datagram = try socket.receive(into: &buffer), commands.isOpen {
                    receive(datagram.bytes, from: datagram.sender, at: transportTime())
                }
            }
        } catch {
            failure = error
        }
    }

    private mutating func receive(_ datagram: [UInt8], from address: IPv4Endpoint, at now: UInt64) {
        if let response = server.receive(datagram, from: address) {
            handleOffline(response, from: address, at: now)
            return
        }

        guard let header = datagram.first, header & 0x80 != 0, let peer = peers[address] else { return }
        do {
            let input = try peer.connection.receive(datagram, from: address, at: now)
            try send(input.replies, to: address)
            peer.lastPacket = now
            if input.opened {
                let connection = RakNetConnection(
                    localAddress: socket.localAddress, key: peer.key, mtu: peer.mtu,
                    messages: peer.messages, commands: commands
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
            if input.closed { remove(address) }
        } catch {
            remove(address, failure: error)
        }
    }

    private mutating func handleOffline(_ response: OfflineServer.Response, from address: IPv4Endpoint, at now: UInt64) {
        do {
            if let negotiation = response.negotiation, peers[address] == nil {
                guard peers.count < maximumConnections else { return }
                let key = TransportPeerKey(address: address, generation: generation)
                generation &+= 1 // reused udp endpoints
                peers[address] = try Peer(key: key, negotiation: negotiation, now: now)
            }
            try socket.send(response.datagram, to: address)
        } catch {
            remove(address, failure: error)
        }
    }

    private mutating func perform(_ command: TransportCommands.Command, at now: UInt64) {
        do {
            switch command {
            case let .send(key, payload, _):
                guard let peer = peers[key.address], peer.key == key, peer.connection.isOpen else {
                    throw RakNetTransportError.closed
                }
                let packets = try peer.connection.send(payload, at: now)
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
