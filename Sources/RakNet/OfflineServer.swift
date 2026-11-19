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

public struct OfflineServer: Sendable {
    public enum ConfigurationError: Error, Equatable {
        case invalidMTU
        case advertisementTooLong
    }

    public struct Response: Sendable {
        public let datagram: [UInt8]
        public let negotiation: Negotiation?
    }

    public struct Negotiation: Sendable {
        public let mtu: UInt16
        public let clientGUID: Int64
    }

    private let guid: [UInt8]
    private let advertisement: [UInt8]
    private let protocolVersion: UInt8
    private let maximumMTU: UInt16

    public init(
        guid: UInt64,
        advertisement: [UInt8],
        protocolVersion: UInt8 = 11,
        maximumMTU: UInt16 = IPv4MTU.defaultValue
    ) throws {
        guard OfflineWire.mtuRange.contains(maximumMTU) else {
            throw ConfigurationError.invalidMTU
        }
        guard advertisement.count <= Int(maximumMTU) - OfflineWire.ipv4UDPOverhead - 35 else {
            throw ConfigurationError.advertisementTooLong
        }
        self.guid = bigEndianBytes(guid)
        self.advertisement = bigEndianBytes(UInt16(advertisement.count)) + advertisement
        self.protocolVersion = protocolVersion
        self.maximumMTU = maximumMTU
    }

    public func reply(to datagram: [UInt8], from sender: IPv4Endpoint) -> [UInt8]? {
        receive(datagram, from: sender)?.datagram
    }

    public func receive(_ datagram: [UInt8], from sender: IPv4Endpoint) -> Response? {
        switch datagram.first {
        case 0x01, 0x02:
            guard datagram.count == 33, datagram[9..<25].elementsEqual(OfflineWire.magic) else {
                return nil
            }
            return Response(datagram: [0x1c] + datagram[1..<9] + guid + OfflineWire.magic + advertisement, negotiation: nil)
        case 0x05:
            guard datagram.count >= 18, datagram[1..<17].elementsEqual(OfflineWire.magic) else {
                return nil
            }
            guard datagram[17] == protocolVersion else {
                return Response(datagram: [0x19, protocolVersion] + OfflineWire.magic + guid, negotiation: nil)
            }
            let headerSize = OfflineWire.ipv4UDPOverhead
            guard datagram.count >= Int(OfflineWire.mtuRange.lowerBound) - headerSize else {
                return nil
            }
            let mtu = min(Int(maximumMTU) - headerSize, datagram.count) + headerSize
            return Response(datagram: [0x06] + OfflineWire.magic + guid + [0] + bigEndianBytes(UInt16(mtu)), negotiation: nil)
        case 0x07:
            guard datagram.count == 34, datagram[1..<17].elementsEqual(OfflineWire.magic),
                  datagram[17] == 4 else {
                return nil
            }
            let requestedMTU = UInt16(wireBytes: datagram[24..<26])
            guard requestedMTU >= OfflineWire.mtuRange.lowerBound else {
                return nil
            }
            let mtu = min(requestedMTU, maximumMTU)
            let reply = [0x08] + OfflineWire.magic + guid + sender.wireBytes + bigEndianBytes(mtu) + [0]
            let clientGUID = Int64(wireBytes: datagram[26..<34])
            return Response(datagram: reply, negotiation: Negotiation(mtu: mtu, clientGUID: clientGUID))
        default:
            return nil
        }
    }
}

func bigEndianBytes<T: FixedWidthInteger>(_ value: T) -> [UInt8] {
    withUnsafeBytes(of: value.bigEndian) { Array($0) }
}

public enum IPv4MTU {
    public static let defaultValue: UInt16 = 1440
}

enum OfflineWire {
    static let magic: [UInt8] = [
        0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe,
        0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78
    ]
    static let ipv4UDPOverhead = 28 // ipv4 and udp headers
    static let mtuRange: ClosedRange<UInt16> = 400...1492
}

extension FixedWidthInteger {
    init(wireBytes: some Collection<UInt8>) {
        self = wireBytes.reduce(0) { ($0 << 8) | Self($1) }
    }
}
