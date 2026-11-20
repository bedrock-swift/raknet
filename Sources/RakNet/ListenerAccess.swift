import Foundation

final class ListenerAccess: @unchecked Sendable {
    private enum Host: Hashable {
        case ipv4(UInt32)
        case ipv6(UInt64, UInt64, scopeID: UInt32)

        init(_ endpoint: RakNetEndpoint) {
            switch endpoint {
            case let .ipv4(address): self = .ipv4(address.address)
            case let .ipv6(address): self = .ipv6(address.addressHigh, address.addressLow, scopeID: address.scopeID)
            }
        }
    }

    private let lock = NSLock()
    private var blocked: [Host: ContinuousClock.Instant] = [:]

    func block(_ endpoint: RakNetEndpoint, for duration: Duration, at now: ContinuousClock.Instant = .now) {
        guard duration > .zero else { return }
        let host = Host(endpoint)
        lock.withLock {
            blocked = blocked.filter { now < $0.value }
            let deadline = now.advanced(by: duration)
            blocked[host] = max(blocked[host] ?? deadline, deadline)
        }
    }

    func unblock(_ endpoint: RakNetEndpoint) {
        lock.withLock { _ = blocked.removeValue(forKey: Host(endpoint)) }
    }

    func isBlocked(_ endpoint: RakNetEndpoint, at now: ContinuousClock.Instant = .now) -> Bool {
        lock.withLock {
            guard !blocked.isEmpty else { return false }
            let host = Host(endpoint)
            guard let deadline = blocked[host] else { return false }
            if now < deadline { return true }
            blocked.removeValue(forKey: host)
            return false
        }
    }
}
