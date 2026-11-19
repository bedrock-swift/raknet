import RakNet
import Testing

private let magic: [UInt8] = [
    0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe,
    0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78
]
private let sender = IPv4Endpoint(address: 0x7f000001, port: 49152)
private let serverGUID: UInt64 = 0x0102030405060708
private let guidBytes: [UInt8] = [1, 2, 3, 4, 5, 6, 7, 8]

@Test(arguments: [UInt8(0x01), UInt8(0x02)])
func pongEchoesTimestampAndAdvertisesBytes(id: UInt8) throws {
    let server = try OfflineServer(guid: serverGUID, advertisement: [0xc3, 0xa9])
    let timestamp: [UInt8] = [0xff, 0, 0, 0, 0, 0, 0, 0x42]
    let ping = [id] + timestamp + magic + guidBytes
    let expected = [0x1c] + timestamp + guidBytes + magic + [0, 2, 0xc3, 0xa9]
    #expect(server.reply(to: ping, from: sender) == expected)
}

@Test func firstReplyNegotiatesMTUAndChecksProtocol() throws {
    let server = try OfflineServer(guid: serverGUID, advertisement: [], maximumMTU: 1200)
    let probe = [0x05] + magic + [11] + Array(repeating: UInt8(0), count: 1446)
    let expected = [0x06] + magic + guidBytes + [0, 0x04, 0xb0]
    #expect(server.reply(to: probe, from: sender) == expected)

    let incompatible = [0x05] + magic + [10]
    let rejection = [0x19, 11] + magic + guidBytes
    #expect(server.reply(to: incompatible, from: sender) == rejection)
    #expect(server.reply(to: Array(probe.prefix(371)), from: sender) == nil)
}

@Test func secondReplyUsesObservedSenderAddress() throws {
    let server = try OfflineServer(guid: serverGUID, advertisement: [])
    let destination: [UInt8] = [4, 0x80, 0xff, 0xff, 0xfe, 0x4a, 0xbc]
    let request = [0x07] + magic + destination + [0x04, 0xb0] + guidBytes
    let clientAddress: [UInt8] = [4, 0x80, 0xff, 0xff, 0xfe, 0xc0, 0]
    let expected = [0x08] + magic + guidBytes + clientAddress + [0x04, 0xb0, 0]
    #expect(server.reply(to: request, from: sender) == expected)

    for length in 0..<request.count {
        #expect(server.reply(to: Array(request.prefix(length)), from: sender) == nil)
    }
    var invalid = request
    invalid[17] = 6
    #expect(server.reply(to: invalid, from: sender) == nil)
}

@Test func malformedPingIsIgnored() throws {
    let server = try OfflineServer(guid: serverGUID, advertisement: [])
    let ping = [0x01] + guidBytes + magic + guidBytes
    var wrongMagic = ping
    wrongMagic[9] = 0xff
    #expect(server.reply(to: wrongMagic, from: sender) == nil)
    #expect(server.reply(to: ping + [0], from: sender) == nil)
}

@Test func invalidConfigurationIsRejected() {
    #expect(throws: OfflineServer.ConfigurationError.invalidMTU) {
        try OfflineServer(guid: serverGUID, advertisement: [], maximumMTU: 1493)
    }
    #expect(throws: OfflineServer.ConfigurationError.advertisementTooLong) {
        try OfflineServer(guid: serverGUID, advertisement: Array(repeating: 0, count: 338), maximumMTU: 400)
    }
}
