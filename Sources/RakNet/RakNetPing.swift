import Dispatch

public struct RakNetPong: Equatable, Sendable {
    public let serverGUID: UInt64
    public let advertisement: [UInt8]
    public let roundTripTime: Duration
}

public extension RakNetConnection {
    static func ping(_ address: IPv4Endpoint, timeout: Duration = .seconds(3)) async throws -> RakNetPong {
        try Task.checkCancellation()
        guard address.address != 0, address.port != 0 else {
            throw RakNetTransportError.invalidRemoteAddress
        }
        guard timeout > .zero else { throw RakNetTransportError.timedOut }

        let socket = try UDPSocket(clientFor: address)
        let result = TransportInbox<RakNetPong>(limit: 1, byteLimit: 1)
        DispatchQueue(label: "raknet.ping").async {
            do {
                try PingExchange.run(to: address, socket: socket, timeout: timeout, result: result)
            } catch {
                result.finish(error)
            }
        }

        do {
            guard let pong = try await result.next() else { throw RakNetTransportError.closed }
            try Task.checkCancellation()
            return pong
        } catch {
            result.finish(error)
            throw error
        }
    }
}

enum PingExchange {
    static func run(
        to address: IPv4Endpoint, socket: UDPSocket, timeout: Duration, result: TransportInbox<RakNetPong>
    ) throws {
        let deadline = ContinuousClock.now.advanced(by: timeout)
        let guid = Int64.random(in: Int64.min ... -1)
        var sentAt = ContinuousClock.now
        var nextProbe = sentAt
        var timestamp: UInt64 = 0
        var buffer = [UInt8](repeating: 0, count: 65_507)

        while !result.isFinished {
            let now = ContinuousClock.now
            guard now < deadline else { throw RakNetTransportError.timedOut }
            if now >= nextProbe {
                timestamp = transportTime()
                sentAt = now
                nextProbe = now.advanced(by: .milliseconds(300))
                try socket.send(request(timestamp: timestamp, guid: guid), to: address)
            }
            guard let datagram = try socket.receive(into: &buffer), datagram.sender == address else { continue }
            let receivedAt = ContinuousClock.now
            guard receivedAt < deadline else { throw RakNetTransportError.timedOut }
            if let pong = response(datagram.bytes, timestamp: timestamp, elapsed: sentAt.duration(to: receivedAt)) {
                _ = result.offer(pong)
                result.finish()
                return
            }
        }
    }

    static func request(timestamp: UInt64, guid: Int64) -> [UInt8] {
        [0x01] + bigEndianBytes(timestamp) + OfflineWire.magic + bigEndianBytes(guid)
    }

    static func response(_ datagram: [UInt8], timestamp: UInt64, elapsed: Duration) -> RakNetPong? {
        guard datagram.count >= 33, datagram[0] == 0x1c,
              UInt64(wireBytes: datagram[1..<9]) == timestamp, // echoed probe timestamp
              datagram[17..<33].elementsEqual(OfflineWire.magic) else { return nil }

        let advertisement: [UInt8]
        if datagram.count == 33 {
            advertisement = [] // legacy pong without size field
        } else {
            guard datagram.count >= 35,
                  datagram.count - 35 == Int(UInt16(wireBytes: datagram[33..<35])) else { return nil }
            advertisement = Array(datagram.dropFirst(35))
        }
        return RakNetPong(
            serverGUID: UInt64(wireBytes: datagram[9..<17]),
            advertisement: advertisement, roundTripTime: elapsed
        )
    }
}
