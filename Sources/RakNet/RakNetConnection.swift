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

    public func close() async throws {
        try await withCheckedThrowingContinuation { continuation in
            commands.submit(.close(key, continuation))
        }
    }
}

final class TransportMetrics: @unchecked Sendable {
    private let lock = NSLock()
    private var measuredRTT: Duration?

    var roundTripTime: Duration? {
        lock.withLock { measuredRTT }
    }

    func update(roundTripTime: Duration?) {
        lock.withLock { measuredRTT = roundTripTime }
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

        func complete(_ result: Result<Void, any Error>) {
            switch self {
            case let .send(_, _, _, _, _, continuation):
                continuation.resume(with: result)
            case let .close(_, continuation):
                continuation?.resume(with: result)
            }
        }

        func stop() {
            if case .close = self {
                complete(.success(()))
            } else {
                complete(.failure(RakNetTransportError.closed))
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
                command.complete(.failure(RakNetTransportError.sendQueueFull))
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
