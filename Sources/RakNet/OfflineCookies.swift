struct OfflineCookies {
    private struct Challenge {
        let value: UInt32
        let expiresAt: UInt64
    }

    private let maximumPending: Int
    private let lifetime: UInt64
    private var pending: [IPv4Endpoint: Challenge] = [:]
    private var nextSweep: UInt64 = 0

    init(maximumPending: Int = 512, lifetime: UInt64 = 5_000) {
        self.maximumPending = maximumPending
        self.lifetime = lifetime
    }

    mutating func issue(to sender: IPv4Endpoint, at now: UInt64) -> UInt32? {
        if let current = value(for: sender, at: now) { return current }
        if pending.count >= maximumPending, now >= nextSweep {
            pending = pending.filter { now < $0.value.expiresAt }
            nextSweep = pending.values.map(\.expiresAt).min() ?? now // next possible expiry
        }
        guard pending.count < maximumPending else { return nil }
        let value = UInt32.random(in: 1...UInt32.max)
        pending[sender] = Challenge(value: value, expiresAt: now + lifetime)
        return value
    }

    mutating func value(for sender: IPv4Endpoint, at now: UInt64) -> UInt32? {
        guard let challenge = pending[sender] else { return nil }
        guard now < challenge.expiresAt else {
            pending.removeValue(forKey: sender)
            return nil
        }
        return challenge.value
    }
}
