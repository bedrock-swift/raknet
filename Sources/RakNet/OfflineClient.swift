public struct OfflineClient: Sendable {
    public enum HandshakeError: Error, Equatable {
        case invalidMTU
        case incompatibleProtocol(UInt8)
    }

    public struct Negotiation: Equatable, Sendable {
        public let serverGUID: UInt64
        public let clientAddress: IPv4Endpoint
        public let mtu: UInt16
    }

    public enum Event: Equatable, Sendable {
        case send([UInt8])
        case negotiated(Negotiation)
    }

    private enum State: Sendable {
        case idle
        case probing(UInt16)
        case opening(serverGUID: UInt64, mtu: UInt16, request: [UInt8])
        case finished
    }

    private let server: IPv4Endpoint
    public let guid: Int64
    private let protocolVersion: UInt8
    private var state = State.idle

    public init(
        server: IPv4Endpoint,
        guid: Int64 = .random(in: Int64.min ... -1), // vanilla client guid sign
        protocolVersion: UInt8 = 11
    ) {
        self.server = server
        self.guid = guid
        self.protocolVersion = protocolVersion
    }

    public mutating func probe(mtu: UInt16 = IPv4MTU.defaultValue) throws -> [UInt8] {
        guard OfflineWire.mtuRange.contains(mtu) else { throw HandshakeError.invalidMTU }
        state = .probing(mtu)
        var probe = [0x05] + OfflineWire.magic + [protocolVersion]
        let size = Int(mtu) - OfflineWire.ipv4UDPOverhead
        probe.append(contentsOf: repeatElement(0, count: size - probe.count))
        return probe
    }

    public var pendingOpenRequest: [UInt8]? {
        guard case let .opening(_, _, request) = state else { return nil }
        return request
    }

    public mutating func receive(_ datagram: [UInt8], from sender: IPv4Endpoint) throws -> Event? {
        guard sender == server else { return nil }
        switch (state, datagram.first) {
        case (.probing, 0x19):
            guard datagram.count == 26, datagram[2..<18].elementsEqual(OfflineWire.magic) else {
                return nil
            }
            state = .finished
            throw HandshakeError.incompatibleProtocol(datagram[1])
        case let (.probing(probeMTU), 0x06):
            guard datagram.count == 28 || datagram.count == 32,
                  datagram[1..<17].elementsEqual(OfflineWire.magic) else {
                return nil
            }
            let security = datagram[25]
            guard (security == 0 && datagram.count == 28) ||
                  (security == 1 && datagram.count == 32) else {
                return nil
            }
            let mtu = UInt16(wireBytes: datagram.suffix(2))
            guard (OfflineWire.mtuRange.lowerBound...probeMTU).contains(mtu) else { return nil }

            let serverGUID = UInt64(wireBytes: datagram[17..<25])
            var request = [0x07] + OfflineWire.magic
            if security == 1 {
                request.append(contentsOf: datagram[26..<30])
                request.append(0) // client security flag
            }
            request += server.wireBytes + bigEndianBytes(mtu) + bigEndianBytes(guid)
            state = .opening(serverGUID: serverGUID, mtu: mtu, request: request)
            return .send(request)
        case let (.opening(serverGUID, offeredMTU, _), 0x08):
            guard datagram.count == 35, datagram[1..<17].elementsEqual(OfflineWire.magic),
                  UInt64(wireBytes: datagram[17..<25]) == serverGUID,
                  datagram[25] == 4, datagram[34] == 0 else {
                return nil
            }
            let mtu = UInt16(wireBytes: datagram[32..<34])
            guard (OfflineWire.mtuRange.lowerBound...offeredMTU).contains(mtu) else { return nil }
            let address = IPv4Endpoint(
                address: ~UInt32(wireBytes: datagram[26..<30]),
                port: UInt16(wireBytes: datagram[30..<32])
            )
            state = .finished
            return .negotiated(Negotiation(serverGUID: serverGUID, clientAddress: address, mtu: mtu))
        default:
            return nil
        }
    }
}
