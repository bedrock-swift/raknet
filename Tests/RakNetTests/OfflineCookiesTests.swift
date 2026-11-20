@testable import RakNet
import Testing

@Test func offlineCookiesReuseChallengesAndExpire() throws {
    let first = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 10001))
    let second = RakNetEndpoint.ipv4(IPv4Endpoint(address: 0x7f000001, port: 10002))
    var cookies = OfflineCookies(maximumPending: 1, lifetime: 5)
    let issued = cookies.issue(to: first, at: 10)
    let value = try #require(issued)

    #expect(cookies.issue(to: first, at: 14) == value)
    #expect(cookies.value(for: second, at: 14) == nil)
    #expect(cookies.issue(to: second, at: 14) == nil)
    #expect(cookies.issue(to: second, at: 15) != nil)
    #expect(cookies.value(for: first, at: 15) == nil)
}

@Test func listenerRejectsInvalidCookieAndDifferentSourcePort() throws {
    let loopback = IPv4Endpoint(address: 0x7f000001, port: 0)
    let listener = try RakNetListener.listen(on: loopback)
    defer { listener.close() }

    let first = try UDPSocket(bindTo: loopback, receiveTimeout: 250)
    let second = try UDPSocket(bindTo: loopback, receiveTimeout: 250)
    var client = OfflineClient(server: listener.localAddress)
    var buffer = [UInt8](repeating: 0, count: 1500)
    try first.send(client.probe(), to: listener.localAddress)
    let reply = try first.receive(into: &buffer)
    let offer = try #require(reply)
    guard case let .send(request) = try client.receive(offer.bytes, from: offer.sender) else {
        Issue.record("missing cookie request")
        return
    }
    try #require(request.count == 39)

    var changed = request
    changed[17] ^= 1
    try first.send(changed, to: listener.localAddress)
    let badCookie = try first.receive(into: &buffer)
    #expect(badCookie == nil)

    try second.send(request, to: listener.localAddress)
    let wrongSource = try second.receive(into: &buffer)
    #expect(wrongSource == nil)

    try first.send(request, to: listener.localAddress)
    let response = try first.receive(into: &buffer)
    let accepted = try #require(response)
    guard case .negotiated = try client.receive(accepted.bytes, from: accepted.sender) else {
        Issue.record("cookie from original port rejected")
        return
    }
}
