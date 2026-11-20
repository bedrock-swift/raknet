import Foundation

public enum RakNetTransportError: Error, Equatable {
    case closed
    case timedOut
    case concurrentReceive
    case receiveBufferFull
    case sendQueueFull
    case invalidConnectionLimit
    case invalidRemoteAddress
}

public struct RakNetSessionSnapshot: Sendable {
    public let localAddress: RakNetEndpoint
    public let mtu: UInt16
    public let bufferedPayloads: [[UInt8]]
    public let capturedAt: UInt64
    public var remoteAddress: RakNetEndpoint { state.peer }

    private let state: OnlineConnection

    init(
        localAddress: RakNetEndpoint, mtu: UInt16, state: OnlineConnection,
        bufferedPayloads: [[UInt8]], capturedAt: UInt64
    ) {
        self.localAddress = localAddress
        self.mtu = mtu
        self.state = state
        self.bufferedPayloads = bufferedPayloads
        self.capturedAt = capturedAt
    }

    public func restore() -> OnlineConnection {
        state
    }
}

public final class RakNetConnection: Sendable {
    public let localAddress: RakNetEndpoint
    public let remoteAddress: RakNetEndpoint
    public let mtu: UInt16

    private let key: TransportPeerKey
    private let messages: TransportInbox<[UInt8]>
    private let commands: TransportCommands
    private let metrics: TransportMetrics

    init(
        localAddress: RakNetEndpoint, key: TransportPeerKey, mtu: UInt16,
        messages: TransportInbox<[UInt8]>, commands: TransportCommands, metrics: TransportMetrics
    ) {
        self.localAddress = localAddress
        remoteAddress = key.address
        self.mtu = mtu
        self.key = key
        self.messages = messages
        self.commands = commands
        self.metrics = metrics
    }

    deinit {
        commands.submit(.close(key, nil))
    }

    public var roundTripTime: Duration? { metrics.roundTripTime }
    public var sendCapacity: Int { metrics.sendCapacity }

    public func maximumMessageLength(for reliability: RakNetReliability = .reliableOrdered) -> Int {
        ConnectedSession.maximumMessageLength(datagramLimit: Int(mtu) - remoteAddress.udpOverhead, reliability: reliability)
    }

    public func send(_ payload: [UInt8]) async throws {
        try await send(payload, reliability: .reliableOrdered)
    }

    public func send(
        _ payload: [UInt8], reliability: RakNetReliability, channel: UInt8 = 0, priority: RakNetPriority = .immediate
    ) async throws {
        try await sendBatch([payload], reliability: reliability, channel: channel, priority: priority)
    }

    public func sendBatch(
        _ payloads: [[UInt8]], reliability: RakNetReliability = .reliableOrdered, channel: UInt8 = 0,
        priority: RakNetPriority = .immediate
    ) async throws {
        try Task.checkCancellation()
        var remainingBytes = 192 * 1024 // command queue budget
        for payload in payloads {
            guard !payload.isEmpty, payload.count <= remainingBytes else {
                throw ConnectedSession.SessionError.invalidPayloadSize
            }
            remainingBytes -= payload.count
        }
        try await withCheckedThrowingContinuation { continuation in
            commands.submit(.send(key, payloads, reliability, channel, priority, continuation))
        }
    }

    public func receive() async throws -> [UInt8]? {
        try await messages.next()
    }

    public func snapshot() async throws -> RakNetSessionSnapshot {
        try Task.checkCancellation()
        let snapshot: RakNetSessionSnapshot = try await withCheckedThrowingContinuation { continuation in
            commands.submit(.snapshot(key, continuation))
        }
        try Task.checkCancellation()
        return snapshot
    }

    public func close() async throws {
        try await withCheckedThrowingContinuation { continuation in
            commands.submit(.close(key, continuation))
        }
    }
}

final class TransportMetrics: @unchecked Sendable {
    private let lock = NSLock()
    private var measuredRTT: Duration?
    private var capacity = 0

    var roundTripTime: Duration? {
        lock.withLock { measuredRTT }
    }

    var sendCapacity: Int {
        lock.withLock { capacity }
    }

    func update(from connection: OnlineConnection) {
        lock.withLock {
            measuredRTT = connection.roundTripTime
            capacity = connection.sendCapacity ?? 0
        }
    }
}

struct TransportPeerKey: Equatable, Sendable {
    let address: RakNetEndpoint
    let generation: UInt64
}

final class TransportCommands: @unchecked Sendable {
    enum Command: Sendable {
        case send(TransportPeerKey, [[UInt8]], RakNetReliability, UInt8, RakNetPriority, CheckedContinuation<Void, any Error>)
        case close(TransportPeerKey, CheckedContinuation<Void, any Error>?)
        case snapshot(TransportPeerKey, CheckedContinuation<RakNetSessionSnapshot, any Error>)

        func fail(_ error: any Error) {
            switch self {
            case let .send(_, _, _, _, _, continuation): continuation.resume(throwing: error)
            case let .close(_, continuation): continuation?.resume(throwing: error)
            case let .snapshot(_, continuation): continuation.resume(throwing: error)
            }
        }

        func stop() {
            if case let .close(_, continuation) = self {
                continuation?.resume()
            } else {
                fail(RakNetTransportError.closed)
            }
        }
    }

    private let lock = NSLock()
    private var open = true
    private var pending: [Command] = []

    func submit(_ command: Command) {
        lock.withLock {
            guard open else {
                command.stop()
                return
            }
            guard pending.count < 256 else {
                command.fail(RakNetTransportError.sendQueueFull)
                return
            }
            pending.append(command)
        }
    }

    var isOpen: Bool {
        lock.withLock { open }
    }

    func drain() -> [Command] {
        lock.withLock {
            guard open else { return [] }
            let commands = pending
            pending = []
            return commands
        }
    }

    func stop() {
        let pending = lock.withLock {
            open = false
            let commands = pending
            pending = []
            return commands
        }
        for command in pending {
            command.stop()
        }
    }
}
