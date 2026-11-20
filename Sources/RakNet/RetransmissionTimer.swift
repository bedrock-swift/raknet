struct RetransmissionTimer: Sendable {
    private static let maximumDelay: UInt64 = 3_000
    private var smoothed: UInt64?
    private var variation: UInt64 = 0

    mutating func observe(_ elapsed: UInt64) {
        let sample = min(max(elapsed, 1), Self.maximumDelay)
        if let smoothed {
            let difference = max(sample, smoothed) - min(sample, smoothed)
            variation = (3 * variation + difference) / 4
            self.smoothed = (7 * smoothed + sample) / 8
        } else {
            smoothed = sample
            variation = sample / 2
        }
    }

    func delay(after retries: Int) -> UInt64 {
        let base = smoothed.map { max(100, $0 + 4 * variation) } ?? 300
        return min(base << min(retries, 4), Self.maximumDelay)
    }
}
