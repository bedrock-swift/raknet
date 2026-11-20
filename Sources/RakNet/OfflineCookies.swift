struct OfflineCookies: Sendable {
    private static let window: UInt64 = 5_000
    private let key0: UInt64
    private let key1: UInt64

    init(key0: UInt64 = .random(in: .min ... .max), key1: UInt64 = .random(in: .min ... .max)) {
        self.key0 = key0
        self.key1 = key1
    }

    func issue(to endpoint: RakNetEndpoint, at time: UInt64) -> UInt32 {
        cookie(to: endpoint, window: time / Self.window)
    }

    func accepts(_ value: UInt32, from endpoint: RakNetEndpoint, at time: UInt64) -> Bool {
        let window = time / Self.window
        let current = cookie(to: endpoint, window: window)
        let previous = window == 0 ? current : cookie(to: endpoint, window: window - 1)
        return value == current || value == previous
    }

    private func cookie(to endpoint: RakNetEndpoint, window: UInt64) -> UInt32 {
        let input = bigEndianBytes(window) + endpoint.socketPeer.wireBytes
        return UInt32(truncatingIfNeeded: Self.tag(input, key0: key0, key1: key1))
    }

    static func tag(_ bytes: [UInt8], key0: UInt64, key1: UInt64) -> UInt64 {
        var v0 = 0x736f_6d65_7073_6575 ^ key0
        var v1 = 0x646f_7261_6e64_6f6d ^ key1
        var v2 = 0x6c79_6765_6e65_7261 ^ key0
        var v3 = 0x7465_6462_7974_6573 ^ key1
        func rotate(_ word: UInt64, by n: Int) -> UInt64 { (word << n) | (word >> (64 - n)) }
        func round() {
            v0 &+= v1
            v1 = rotate(v1, by: 13) ^ v0
            v0 = rotate(v0, by: 32)
            v2 &+= v3
            v3 = rotate(v3, by: 16) ^ v2
            v0 &+= v3
            v3 = rotate(v3, by: 21) ^ v0
            v2 &+= v1
            v1 = rotate(v1, by: 17) ^ v2
            v2 = rotate(v2, by: 32)
        }
        func absorb(_ word: UInt64) {
            v3 ^= word
            round()
            round()
            v0 ^= word
        }
        var tail: UInt64 = 0
        for (index, byte) in bytes.enumerated() {
            let shift = (index % 8) * 8
            tail |= UInt64(byte) << shift
            if shift == 56 {
                absorb(tail)
                tail = 0
            }
        }
        absorb(tail | (UInt64(bytes.count & 255) << 56))
        v2 ^= 0xff
        for _ in 0..<4 { round() }
        return v0 ^ v1 ^ v2 ^ v3
    }
}
