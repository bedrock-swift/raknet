struct CongestionWindow: Sendable {
    private let datagramSize: Int
    private let maximum: Int
    private var slowStartThreshold: Int
    private var recoverySequence: UInt32?

    private(set) var bytes: Int

    init(datagramSize: Int) {
        self.datagramSize = datagramSize
        maximum = 256 * datagramSize
        slowStartThreshold = maximum
        bytes = datagramSize
    }

    mutating func acknowledge(sequence: UInt32, bytes acknowledged: Int) {
        if let recoverySequence {
            let distance = (sequence &- recoverySequence) & 0x00ff_ffff
            guard distance < 0x0080_0000 else { return } // 24 bit recovery boundary
            self.recoverySequence = nil
        }
        let increase = bytes < slowStartThreshold ? acknowledged : max(1, datagramSize * acknowledged / bytes)
        bytes = min(bytes + increase, maximum)
    }

    mutating func reportLoss(nextSequence: UInt32) {
        guard recoverySequence == nil else { return }
        recoverySequence = nextSequence
        slowStartThreshold = max(bytes / 2, datagramSize)
        bytes = datagramSize
    }
}
