import Foundation

public enum RakNetTransportError: Error, Equatable {
    case closed
    case timedOut
    case concurrentReceive
    case receiveBufferFull
    case sendQueueFull
    case invalidConnectionLimit
    case invalidRemoteAddress
    case snapshotAddressMismatch
    case connectionAlreadyExists
    case connectionLimitReached
}

public struct RakNetSessionSnapshot: Sendable {
    public enum ArchiveError: Error, Equatable {
        case invalidState
        case unsupportedVersion(UInt8)
        case sizeLimit
    }

    private struct Archive: Codable {
        let version: UInt8
        let localAddress: RakNetEndpoint
        let remoteAddress: RakNetEndpoint
        let mtu: UInt16
        let capturedAt: UInt64
        let bufferedPayloads: [[UInt8]]
        let connection: OnlineConnection.Archive
    }

    private static let archiveVersion: UInt8 = 1
    private static let archiveSizeLimit = 32 * 1024 * 1024

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

    public init(data: Data) throws {
        guard data.count <= Self.archiveSizeLimit else { throw ArchiveError.sizeLimit }
        let archive = try PropertyListDecoder().decode(Archive.self, from: data)
        guard archive.version == Self.archiveVersion else { throw ArchiveError.unsupportedVersion(archive.version) }
        guard archive.remoteAddress.port != 0, !archive.remoteAddress.isUnspecified,
              archive.localAddress.udpOverhead == archive.remoteAddress.udpOverhead,
              archive.bufferedPayloads.count <= 64,
              archive.bufferedPayloads.allSatisfy({ !$0.isEmpty && $0.count <= 512 * 1024 }),
              archive.bufferedPayloads.reduce(0, { $0 + $1.count }) <= 512 * 1024 else {
            throw ArchiveError.invalidState
        }
        let connection = try OnlineConnection(
            restoring: archive.remoteAddress, mtu: archive.mtu, at: archive.capturedAt, archive: archive.connection
        )
        self.init(
            localAddress: archive.localAddress, mtu: archive.mtu, state: connection,
            bufferedPayloads: archive.bufferedPayloads, capturedAt: archive.capturedAt
        )
    }

    public func encoded() throws -> Data {
        let archive = Archive(
            version: Self.archiveVersion, localAddress: localAddress, remoteAddress: remoteAddress, mtu: mtu,
            capturedAt: capturedAt, bufferedPayloads: bufferedPayloads, connection: try state.archive(at: capturedAt)
        )
        let data = try PropertyListEncoder().encode(archive)
        guard data.count <= Self.archiveSizeLimit else { throw ArchiveError.sizeLimit }
        return data
    }

    public func restore() -> OnlineConnection {
        state
    }

    public func restore(at milliseconds: UInt64) -> OnlineConnection {
        var connection = state
        connection.rebaseClock(from: capturedAt, to: milliseconds)
        return connection
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

    public func maximumUnfragmentedMessageLength(for reliability: RakNetReliability = .reliableOrdered) -> Int {
        ConnectedSession.payloadLimit(datagramLimit: Int(mtu) - remoteAddress.udpOverhead, reliability: reliability)
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
            commands.submit(.snapshot(key, detach: false, continuation))
        }
        try Task.checkCancellation()
        return snapshot
    }

    public func detach() async throws -> RakNetSessionSnapshot {
        try Task.checkCancellation()
        return try await withCheckedThrowingContinuation { continuation in
            commands.submit(.snapshot(key, detach: true, continuation))
        }
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

struct TransportPeerKey: Hashable, Sendable {
    let address: RakNetEndpoint
    let generation: UInt64
}

final class TransportCommands: @unchecked Sendable {
    enum Command: Sendable {
        case send(TransportPeerKey, [[UInt8]], RakNetReliability, UInt8, RakNetPriority, CheckedContinuation<Void, any Error>)
        case close(TransportPeerKey, CheckedContinuation<Void, any Error>?)
        case snapshot(TransportPeerKey, detach: Bool, CheckedContinuation<RakNetSessionSnapshot, any Error>)
        case adopt(RakNetSessionSnapshot, CheckedContinuation<RakNetConnection, any Error>)

        func fail(_ error: any Error) {
            switch self {
            case let .send(_, _, _, _, _, continuation): continuation.resume(throwing: error)
            case let .close(_, continuation): continuation?.resume(throwing: error)
            case let .snapshot(_, _, continuation): continuation.resume(throwing: error)
            case let .adopt(_, continuation): continuation.resume(throwing: error)
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
    private var wake: (@Sendable () -> Void)?
    private let capacity: Int
    private var sendsByPeer: [TransportPeerKey: Int] = [:]
    private var sendBytes = 0

    init(peerLimit: Int = 1) {
        capacity = max(256, min(peerLimit, 512) * 8)
    }

    func installWake(_ wake: @escaping @Sendable () -> Void) {
        lock.withLock { if open { self.wake = wake } }
    }

    func submit(_ command: Command) {
        let wake = lock.withLock { () -> (@Sendable () -> Void)? in
            guard open else {
                command.stop()
                return nil
            }
            guard reserve(command) else {
                command.fail(RakNetTransportError.sendQueueFull)
                return nil
            }
            let notify = pending.isEmpty
            pending.append(command)
            return notify ? self.wake : nil
        }
        wake?()
    }

    private func reserve(_ command: Command) -> Bool {
        guard case let .send(key, payloads, _, _, _, _) = command else {
            return pending.count < capacity + 64 // control command reserve
        }
        let bytes = payloads.reduce(0) { $0 + $1.count }
        guard pending.count < capacity, sendsByPeer[key, default: 0] < 64,
              bytes <= 48 * 1024 * 1024 - sendBytes else { return false }
        sendsByPeer[key, default: 0] += 1
        sendBytes += bytes
        return true
    }

    private func resetSendBudget() {
        sendsByPeer.removeAll(keepingCapacity: true)
        sendBytes = 0
    }

    var isOpen: Bool {
        lock.withLock { open }
    }

    func drain() -> [Command] {
        lock.withLock {
            guard open else { return [] }
            let commands = pending
            pending = []
            resetSendBudget()
            return commands
        }
    }

    func stop() {
        let (pending, wake) = lock.withLock {
            open = false
            let commands = pending
            let notify = wake
            pending = []
            resetSendBudget()
            wake = nil
            return (commands, notify)
        }
        wake?()
        for command in pending {
            command.stop()
        }
    }
}
