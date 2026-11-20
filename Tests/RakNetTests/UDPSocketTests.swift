@testable import RakNet
import Testing

@Test func udpSocketReportsBoundPortAndReceivesDatagram() throws {
    let loopback = IPv4Endpoint(address: 0x7f000001, port: 0)
    let sender = try UDPSocket(bindTo: loopback)
    let receiver = try UDPSocket(bindTo: loopback)
    #expect(receiver.localAddress == .ipv4(IPv4Endpoint(address: loopback.address, port: receiver.localAddress.port)))
    #expect(receiver.localAddress.port != 0)
    #expect(sender.localAddress.port != receiver.localAddress.port)

    let payload: [UInt8] = [0xfe, 0, 0xff]
    try sender.send(payload, to: receiver.localAddress)

    var buffer = [UInt8](repeating: 0, count: 32)
    let received = try receiver.receive(into: &buffer)
    let datagram = try #require(received)
    #expect(datagram.bytes == payload)
    #expect(datagram.sender == sender.localAddress)
}

@Test func commandsWakeAnIdleSocketAndCoalesceQueuedWork() throws {
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: 0)
    let receiver = try UDPSocket(bindTo: endpoint, receiveTimeout: 80)
    let sender = try UDPSocket(bindTo: endpoint)
    let commands = TransportCommands()
    commands.installWake { try? sender.send([0], to: receiver.localAddress) }
    let key = TransportPeerKey(address: receiver.localAddress, generation: 0)
    var buffer = [UInt8](repeating: 0, count: 32)
    commands.submit(.close(key, nil))
    commands.submit(.close(key, nil))
    let first = try receiver.receive(into: &buffer)
    #expect(first?.bytes == [0])
    let duplicate = try receiver.receive(into: &buffer)
    #expect(duplicate == nil)
    #expect(commands.drain().count == 2)
    commands.stop()
    let shutdown = try receiver.receive(into: &buffer)
    #expect(shutdown?.bytes == [0])
}

@Test func commandQueueAdmitsFanoutAndLimitsOnePeer() async {
    let commands = TransportCommands(peerLimit: 128)
    let submitted = AsyncStream<Void>.makeStream()
    let total = 380
    let pending = Task {
        await withTaskGroup(of: Bool.self) { group in
            for i in 0..<total {
                let port = i < 300 ? UInt16(2_000 + i) : 3_000
                let endpoint = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: port))
                let key = TransportPeerKey(address: endpoint, generation: 0)
                group.addTask {
                    do {
                        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                            commands.submit(.send(key, [[0xfe]], .reliableOrdered, 0, .immediate, continuation))
                            _ = submitted.continuation.yield(())
                        }
                        return false
                    } catch {
                        return error as? RakNetTransportError == .sendQueueFull
                    }
                }
            }
            var rejected = 0
            for await didReject in group where didReject { rejected += 1 }
            return rejected
        }
    }
    for await _ in submitted.stream.prefix(total) {}
    let queued = commands.drain()
    for command in queued { command.stop() }
    #expect(queued.count == 364)
    #expect(await pending.value == 16)
}
