import Foundation

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

enum ListenerHost: Hashable {
    case ipv4(UInt32)
    case ipv6(UInt64, UInt64, scopeID: UInt32)

    init(_ endpoint: RakNetEndpoint) {
        switch endpoint {
        case let .ipv4(address): self = .ipv4(address.address)
        case let .ipv6(address): self = .ipv6(address.addressHigh, address.addressLow, scopeID: address.scopeID)
        }
    }
}

struct OfflinePacketRate {
    let limits: RakNetListener.OfflinePacketLimits

    private var windowStart: UInt64?
    private var total: UInt32 = 0
    private var byHost: [ListenerHost: UInt32] = [:]

    init(limits: RakNetListener.OfflinePacketLimits) {
        self.limits = limits
    }

    mutating func allow(_ endpoint: RakNetEndpoint, at now: UInt64) -> Bool {
        if windowStart.map({ now - $0 >= 1_000 }) ?? true {
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
