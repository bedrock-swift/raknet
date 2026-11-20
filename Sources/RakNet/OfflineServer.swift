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
    let advertisement: [UInt8]
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
        self.guid = bigEndianBytes(guid)
        self.advertisement = try Self.encodeAdvertisement(advertisement, maximumMTU: maximumMTU)
        self.protocolVersion = protocolVersion
        self.maximumMTU = maximumMTU
    }

    static func encodeAdvertisement(_ advertisement: [UInt8], maximumMTU: UInt16) throws -> [UInt8] {
        guard advertisement.count <= Int(maximumMTU) - OfflineWire.ipv4UDPOverhead - 35 else {
            throw ConfigurationError.advertisementTooLong
        }
        return bigEndianBytes(UInt16(advertisement.count)) + advertisement
    }

    public func reply(to datagram: [UInt8], from sender: IPv4Endpoint) -> [UInt8]? {
        receive(datagram, from: sender)?.datagram
    }

    public func receive(_ datagram: [UInt8], from sender: IPv4Endpoint) -> Response? {
        receive(datagram, from: sender, cookie: nil)
    }

    func receive(
        _ datagram: [UInt8], from sender: IPv4Endpoint, cookie: UInt32?,
        advertisement override: (() -> [UInt8])? = nil
    ) -> Response? {
        switch datagram.first {
        case 0x01, 0x02:
            guard datagram.count == 33, datagram[9..<25].elementsEqual(OfflineWire.magic) else {
                return nil
            }
            let data = override?() ?? advertisement
            return Response(datagram: [0x1c] + datagram[1..<9] + guid + OfflineWire.magic + data, negotiation: nil)
        case OfflineWire.openRequestOne:
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
            var reply = [OfflineWire.openReplyOne] + OfflineWire.magic + guid
            reply.append(cookie == nil ? 0 : 1)
            if let cookie { reply += bigEndianBytes(cookie) }
            reply += bigEndianBytes(UInt16(mtu))
            return Response(datagram: reply, negotiation: nil)
        case OfflineWire.openRequestTwo:
            let address = cookie == nil ? 17 : 22 // cookie and client security flag
            guard datagram.count == address + 17, datagram[1..<17].elementsEqual(OfflineWire.magic),
                  datagram[address] == 4 else {
                return nil
            }
            if let cookie, UInt32(wireBytes: datagram[17..<21]) != cookie { return nil }
            let mtuOffset = address + 7
            let requestedMTU = UInt16(wireBytes: datagram[mtuOffset..<(mtuOffset + 2)])
            guard requestedMTU >= OfflineWire.mtuRange.lowerBound else {
                return nil
            }
            let mtu = min(requestedMTU, maximumMTU)
            let reply = [0x08] + OfflineWire.magic + guid + sender.wireBytes + bigEndianBytes(mtu) + [0]
            let clientGUID = Int64(wireBytes: datagram.suffix(8))
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
    static let openRequestOne: UInt8 = 0x05
    static let openReplyOne: UInt8 = 0x06
    static let openRequestTwo: UInt8 = 0x07
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
