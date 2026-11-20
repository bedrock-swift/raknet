public struct IPv4Endpoint: Hashable, Sendable {
    public let address: UInt32
    public let port: UInt16

    public init(address: UInt32, port: UInt16) {
        self.address = address
        self.port = port
    }

    var wireBytes: [UInt8] {
        [4] + bigEndianBytes(~address) + bigEndianBytes(port)
    }
}

public struct IPv6Endpoint: Hashable, Sendable {
    public let addressHigh: UInt64
    public let addressLow: UInt64
    public let port: UInt16
    public let flowInfo: UInt32
    public let scopeID: UInt32

    public init(addressHigh: UInt64, addressLow: UInt64, port: UInt16, flowInfo: UInt32 = 0, scopeID: UInt32 = 0) {
        self.addressHigh = addressHigh
        self.addressLow = addressLow
        self.port = port
        self.flowInfo = flowInfo
        self.scopeID = scopeID
    }

    var wireBytes: [UInt8] {
        let header = [UInt8(6), 23, 0] + bigEndianBytes(port) // little endian family
        return header + bigEndianBytes(flowInfo) + bigEndianBytes(addressHigh)
            + bigEndianBytes(addressLow) + bigEndianBytes(scopeID)
    }
}

public enum RakNetEndpoint: Hashable, Sendable {
    case ipv4(IPv4Endpoint)
    case ipv6(IPv6Endpoint)

    public var port: UInt16 {
        switch self {
        case let .ipv4(endpoint): endpoint.port
        case let .ipv6(endpoint): endpoint.port
        }
    }

    var wireBytes: [UInt8] {
        switch self {
        case let .ipv4(endpoint): endpoint.wireBytes
        case let .ipv6(endpoint): endpoint.wireBytes
        }
    }

    var udpOverhead: Int {
        switch self {
        case .ipv4: OfflineWire.ipv4UDPOverhead
        case .ipv6: 48 // ipv6 and udp headers
        }
    }

    init?(reading reader: inout DatagramReader) {
        switch reader.byte() {
        case 4:
            guard let address = reader.integer(UInt32.self), let port = reader.integer(UInt16.self) else { return nil }
            self = .ipv4(IPv4Endpoint(address: ~address, port: port))
        case 6:
            guard reader.read(2) != nil,
                  let port = reader.integer(UInt16.self), let flow = reader.integer(UInt32.self),
                  let high = reader.integer(UInt64.self), let low = reader.integer(UInt64.self),
                  let scope = reader.integer(UInt32.self) else { return nil }
            self = .ipv6(IPv6Endpoint(addressHigh: high, addressLow: low, port: port, flowInfo: flow, scopeID: scope))
        default:
            return nil
        }
    }
}
