import RakNet
import Testing

@Test func udpSocketReportsBoundPortAndReceivesDatagram() throws {
    let loopback = IPv4Endpoint(address: 0x7f000001, port: 0)
    let sender = try UDPSocket(bindTo: loopback)
    let receiver = try UDPSocket(bindTo: loopback)
    #expect(receiver.localAddress.address == loopback.address)
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
