import Dispatch

public struct IPv4Endpoint: Hashable, Sendable, Codable {
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

public struct IPv6Endpoint: Hashable, Sendable, Codable {
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

public enum RakNetEndpoint: Hashable, Sendable, Codable {
    case ipv4(IPv4Endpoint)
    case ipv6(IPv6Endpoint)

    public enum ResolutionError: Error, Equatable {
        case invalidHost
        case lookupFailed(Int32)
        case noAddresses
    }

    public static func resolve(_ host: String, port: UInt16) async throws -> [RakNetEndpoint] {
        try Task.checkCancellation()
        guard !host.isEmpty, !host.utf8.contains(0) else { throw ResolutionError.invalidHost }
        let result = TransportInbox<[RakNetEndpoint]>(limit: 1, byteLimit: 1)
        DispatchQueue.global(qos: .userInitiated).async {
            do {
                _ = result.offer(try UDPSocket.resolve(host, port: port))
                result.finish()
            } catch {
                result.finish(error)
            }
        }
        guard let addresses = try await result.next() else { throw ResolutionError.noAddresses }
        try Task.checkCancellation()
        return addresses
    }

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

    var socketPeer: RakNetEndpoint {
        guard case let .ipv6(endpoint) = self, endpoint.flowInfo != 0 else { return self }
        return .ipv6(IPv6Endpoint(
            addressHigh: endpoint.addressHigh, addressLow: endpoint.addressLow, port: endpoint.port,
            scopeID: endpoint.scopeID
        )) // socket peer identity
    }

    var forLocalWake: RakNetEndpoint {
        guard isUnspecified else { return self }
        switch self {
        case .ipv4: return .ipv4(IPv4Endpoint(address: 0x7f000001, port: port))
        case .ipv6: return .ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 1, port: port))
        }
    }

    var isUnspecified: Bool {
        switch self {
        case let .ipv4(endpoint): endpoint.address == 0
        case let .ipv6(endpoint): endpoint.addressHigh == 0 && endpoint.addressLow == 0
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

func withResolvedPeer<Value: Sendable>(
    _ host: String, port: UInt16, timeout: Duration,
    _ operation: @escaping @Sendable (RakNetEndpoint, Duration) async throws -> Value
) async throws -> Value {
    try Task.checkCancellation()
    guard port != 0 else { throw RakNetTransportError.invalidRemoteAddress }
    guard timeout > .zero else { throw RakNetTransportError.timedOut }
    let deadline = ContinuousClock.now.advanced(by: timeout)

    return try await withThrowingTaskGroup(of: Value.self) { group in
        group.addTask {
            let endpoints = try await RakNetEndpoint.resolve(host, port: port)
            return try await withThrowingTaskGroup(of: Result<Value, any Error>.self) { attempts in
                let stagger = min(.milliseconds(150), timeout / 4)
                for (index, endpoint) in endpoints.enumerated() {
                    attempts.addTask {
                        do {
                            if index > 0 { try await Task.sleep(for: stagger * min(index, 3)) }
                            let remaining = ContinuousClock.now.duration(to: deadline)
                            guard remaining > .zero else { throw RakNetTransportError.timedOut }
                            return .success(try await operation(endpoint, remaining))
                        } catch {
                            return .failure(error)
                        }
                    }
                }

                defer { attempts.cancelAll() }
                var failure: (any Error)?
                while let result = try await attempts.next() {
                    try Task.checkCancellation()
                    switch result {
                    case let .success(value): return value
                    case let .failure(error): if failure == nil { failure = error }
                    }
                }
                throw failure ?? RakNetEndpoint.ResolutionError.noAddresses
            }
        }
        group.addTask {
            try await ContinuousClock().sleep(until: deadline)
            throw RakNetTransportError.timedOut
        }
        defer { group.cancelAll() }
        guard let value = try await group.next() else { throw CancellationError() }
        return value
    }
}
