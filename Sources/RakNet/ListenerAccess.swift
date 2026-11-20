import Foundation

public struct RakNetOfflinePacketLimits: Equatable, Sendable {
    public let perIP: UInt32
    public let total: UInt32

    public init(perIP: UInt32 = 120, total: UInt32 = 100_000) {
        self.perIP = perIP
        self.total = total
    }
}

final class ListenerAccess: @unchecked Sendable {
    private let lock = NSLock()
    private var blocked: [ListenerHost: ContinuousClock.Instant] = [:]

    func block(_ endpoint: RakNetEndpoint, for duration: Duration, at now: ContinuousClock.Instant = .now) {
        guard duration > .zero else { return }
        let host = ListenerHost(endpoint)
        lock.withLock {
            blocked = blocked.filter { now < $0.value }
            let deadline = now.advanced(by: duration)
            blocked[host] = max(blocked[host] ?? deadline, deadline)
        }
    }

    func unblock(_ endpoint: RakNetEndpoint) {
        lock.withLock { _ = blocked.removeValue(forKey: ListenerHost(endpoint)) }
    }

    func isBlocked(_ endpoint: RakNetEndpoint, at now: ContinuousClock.Instant = .now) -> Bool {
        lock.withLock {
            guard !blocked.isEmpty else { return false }
            let host = ListenerHost(endpoint)
            guard let deadline = blocked[host] else { return false }
            if now < deadline { return true }
            blocked.removeValue(forKey: host)
            return false
        }
    }
}

enum ListenerHost: Hashable, Sendable {
    case ipv4(UInt32)
    case ipv6(UInt64, UInt64, scopeID: UInt32)

    init(_ endpoint: RakNetEndpoint) {
        switch endpoint {
        case let .ipv4(address): self = .ipv4(address.address)
        case let .ipv6(address): self = .ipv6(address.addressHigh, address.addressLow, scopeID: address.scopeID)
        }
    }
}

struct OfflinePacketRate: Sendable {
    let limits: RakNetOfflinePacketLimits

    private var windowStart: UInt64?
    private var total: UInt32 = 0
    private var byHost: [ListenerHost: UInt32] = [:]

    init(limits: RakNetOfflinePacketLimits) {
        self.limits = limits
    }

    mutating func allow(_ endpoint: RakNetEndpoint, at now: UInt64) -> Bool {
        if windowStart.map({ now < $0 || now - $0 >= 1_000 }) ?? true {
            windowStart = now
            total = 0
            byHost.removeAll(keepingCapacity: true)
        }
        guard total < limits.total else { return false }
        let host = ListenerHost(endpoint)
        let count = byHost[host, default: 0]
        guard count < limits.perIP else { return false }
        byHost[host] = count + 1
        total += 1
        return true
    }
}

struct OnlinePacketRate: Sendable {
    private let packetsPerSecond: UInt64
    private let capacity: UInt64
    private var credit: UInt64
    private var updatedAt: UInt64
    private var blockedUntil: UInt64 = 0

    init(packetsPerSecond: UInt32, at now: UInt64) {
        self.packetsPerSecond = UInt64(packetsPerSecond)
        capacity = UInt64(packetsPerSecond) * 1_000
        credit = capacity
        updatedAt = now
    }

    mutating func allow(at now: UInt64) -> Bool {
        guard now >= blockedUntil else { return false }
        let elapsed = now >= updatedAt ? min(now - updatedAt, 1_000) : 0
        credit = min(capacity, credit + elapsed * packetsPerSecond)
        updatedAt = max(updatedAt, now)
        guard credit >= 1_000 else {
            blockedUntil = now + min(10_000, UInt64.max - now)
            return false
        }
        credit -= 1_000
        return true
    }
}

struct ConnectionQuota: Sendable {
    let maximum: Int
    private let perIP: Int?
    private var total = 0
    private var byHost: [ListenerHost: Int] = [:]

    init(maximum: Int, perIP: Int?) throws {
        guard (1...4096).contains(maximum), perIP.map({ (1...4096).contains($0) }) ?? true else {
            throw RakNetTransportError.invalidConnectionLimit
        }
        self.maximum = maximum
        self.perIP = perIP
    }

    mutating func reserve(_ endpoint: RakNetEndpoint) throws {
        guard total < maximum else { throw RakNetTransportError.connectionLimitReached }
        if let perIP {
            let host = ListenerHost(endpoint)
            guard byHost[host, default: 0] < perIP else { throw RakNetTransportError.connectionLimitReached }
            byHost[host, default: 0] += 1
        }
        total += 1
    }

    mutating func release(_ endpoint: RakNetEndpoint) {
        total -= 1
        let host = ListenerHost(endpoint)
        if let count = byHost[host] {
            byHost[host] = count > 1 ? count - 1 : nil
        }
    }
}
