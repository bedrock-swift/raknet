public extension RakNetConnection {
    static func dial(
        _ address: IPv4Endpoint,
        mtu: UInt16 = IPv4MTU.defaultValue,
        timeout: Duration = .seconds(5)
    ) async throws -> RakNetConnection {
        try Task.checkCancellation()
        guard address.address != 0, address.port != 0 else {
            throw RakNetTransportError.invalidRemoteAddress
        }
        guard OfflineWire.mtuRange.contains(mtu) else { throw OfflineClient.HandshakeError.invalidMTU }
        guard timeout > .zero else { throw RakNetTransportError.timedOut }

        let socket = try UDPSocket(clientFor: address)
        let accepted = TransportInbox<RakNetConnection>(limit: 1, byteLimit: 1)
        let commands = TransportCommands()
        TransportDriver.launch(
            socket: socket, role: .dial(address, mtu: mtu, timeout: timeout),
            accepted: accepted, commands: commands
        )

        do {
            guard let connection = try await accepted.next() else { throw RakNetTransportError.closed }
            try Task.checkCancellation()
            return connection
        } catch {
            commands.stop()
            throw error
        }
    }
}

struct DialAttempt {
    let address: IPv4Endpoint
    let timeout: Duration
    private let started = ContinuousClock.now
    private let sizes: [UInt16]
    private var client: OfflineClient
    private var probe: [UInt8] = []
    private var probes = 0
    private var nextSend: UInt64 = 0

    init(address: IPv4Endpoint, mtu: UInt16, timeout: Duration) {
        self.address = address
        self.timeout = timeout
        sizes = [mtu] + [UInt16(1200), 576].filter { $0 < mtu }
        client = OfflineClient(server: address)
    }

    var expired: Bool { started.duration(to: .now) >= timeout }
    var guid: Int64 { client.guid }

    mutating func scheduledPacket(at now: UInt64) throws -> [UInt8]? {
        guard now >= nextSend else { return nil }
        nextSend = now + 300
        if let request = client.pendingOpenRequest { return request }
        if probes % 3 == 0 {
            probe = try client.probe(mtu: sizes[min(probes / 3, sizes.count - 1)]) // smaller mtu after unanswered probes
        }
        probes += 1
        return probe
    }

    mutating func receive(_ datagram: [UInt8], from sender: IPv4Endpoint, at now: UInt64) throws -> OfflineClient.Event? {
        let event = try client.receive(datagram, from: sender)
        if case .send = event { nextSend = now + 300 }
        return event
    }
}
