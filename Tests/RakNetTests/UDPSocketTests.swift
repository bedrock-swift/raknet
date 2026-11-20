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
