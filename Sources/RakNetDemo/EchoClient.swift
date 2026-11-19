import RakNet

enum EchoClientError: Error {
    case timeout
    case unexpectedPayload
}

func runEchoClient(port: UInt16) async throws {
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: port)
    let connection = try await RakNetConnection.dial(endpoint)
    let payload = (0..<4096).map { UInt8(truncatingIfNeeded: $0 * 19 + 0xfe) }

    do {
        try await connection.send(payload)
        let echo = try await withThrowingTaskGroup(of: [UInt8]?.self) { group in
            group.addTask { try await connection.receive() }
            group.addTask {
                try await Task.sleep(for: .seconds(5))
                throw EchoClientError.timeout
            }
            defer { group.cancelAll() }
            return try await group.next() ?? nil
        }
        guard echo == payload else { throw EchoClientError.unexpectedPayload }
        try await connection.close()
        print("Received \(payload.count) echoed bytes")
    } catch {
        try? await connection.close()
        throw error
    }
}
