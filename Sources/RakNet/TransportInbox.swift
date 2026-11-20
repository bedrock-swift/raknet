import Foundation

final class TransportInbox<Element: Sendable>: @unchecked Sendable {
    private struct Waiter {
        let id: UUID
        let continuation: CheckedContinuation<Element?, any Error>
    }

    private let lock = NSLock()
    private let limit: Int
    private let byteLimit: Int
    private var buffered: [(value: Element, size: Int)] = []
    private var bytes = 0
    private var waiter: Waiter?
    private var finished = false
    private var failure: (any Error)?

    init(limit: Int, byteLimit: Int) {
        self.limit = limit
        self.byteLimit = byteLimit
    }

    var isFinished: Bool { lock.withLock { finished } }

    func offer(_ value: Element, size: Int = 1) -> Bool {
        lock.withLock {
            guard !finished else { return false }
            if let waiter {
                self.waiter = nil
                waiter.continuation.resume(returning: value)
                return true
            }
            guard buffered.count < limit, size <= byteLimit - bytes else { return false }
            buffered.append((value, size))
            bytes += size
            return true
        }
    }

    func next() async throws -> Element? {
        let id = UUID() // cancel only this receive
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                lock.withLock {
                    if Task.isCancelled {
                        continuation.resume(throwing: CancellationError())
                    } else if !buffered.isEmpty {
                        let item = buffered.removeFirst()
                        bytes -= item.size
                        continuation.resume(returning: item.value)
                    } else if finished {
                        if let failure {
                            continuation.resume(throwing: failure)
                        } else {
                            continuation.resume(returning: nil)
                        }
                    } else if waiter != nil {
                        continuation.resume(throwing: RakNetTransportError.concurrentReceive)
                    } else {
                        waiter = Waiter(id: id, continuation: continuation)
                    }
                }
            }
        } onCancel: {
            self.cancel(id)
        }
    }

    func bufferedValues() -> [Element] {
        lock.withLock { buffered.map(\.value) }
    }

    func drainAndFinish() -> [Element] {
        lock.withLock {
            let values = buffered.map(\.value)
            finishLocked(nil, discardBuffered: true)
            return values
        }
    }

    func finish(_ error: (any Error)? = nil, discardBuffered: Bool = false) {
        lock.withLock { finishLocked(error, discardBuffered: discardBuffered) }
    }

    private func finishLocked(_ error: (any Error)?, discardBuffered: Bool) {
        guard !finished else { return }
        finished = true
        failure = error
        if discardBuffered {
            buffered.removeAll()
            bytes = 0
        }
        if let waiter {
            self.waiter = nil
            if let error {
                waiter.continuation.resume(throwing: error)
            } else {
                waiter.continuation.resume(returning: nil)
            }
        }
    }

    private func cancel(_ id: UUID) {
        lock.withLock {
            guard let waiter, waiter.id == id else { return }
            self.waiter = nil
            waiter.continuation.resume(throwing: CancellationError())
        }
    }
}
