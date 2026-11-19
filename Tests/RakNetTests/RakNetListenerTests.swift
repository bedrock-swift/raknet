import RakNet
import Testing

@Test func listenerCloseEndsAccept() async throws {
    let address = IPv4Endpoint(address: 0x7f000001, port: 0)
    let listener = try RakNetListener.listen(on: address)
    #expect(listener.localAddress.port != 0)

    let waiting = Task { try await listener.accept() }
    listener.close()
    let connection = try await waiting.value
    #expect(connection == nil)
}

@Test func listenerCancellationDoesNotCloseListener() async throws {
    let address = IPv4Endpoint(address: 0x7f000001, port: 0)
    let listener = try RakNetListener.listen(on: address)
    defer { listener.close() }

    let abandoned = Task { try await listener.accept() }
    abandoned.cancel()
    await #expect(throws: CancellationError.self) {
        try await abandoned.value
    }

    let waiting = Task { try await listener.accept() }
    listener.close()
    let connection = try await waiting.value
    #expect(connection == nil)
}
