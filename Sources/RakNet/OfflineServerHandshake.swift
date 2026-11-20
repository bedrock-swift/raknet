public struct OfflineServerHandshake: Sendable {
    private let server: OfflineServer
    private let useCookies: Bool
    private let cookies = OfflineCookies()

    public init(server: OfflineServer, useCookies: Bool = true) {
        self.server = server
        self.useCookies = useCookies
    }

    public mutating func receive(
        _ datagram: [UInt8], from address: RakNetEndpoint, at milliseconds: UInt64
    ) -> OfflineServer.Response? {
        receive(datagram, from: address, at: milliseconds, advertisement: nil)
    }

    mutating func receive(
        _ datagram: [UInt8], from address: RakNetEndpoint, at milliseconds: UInt64, advertisement: (() -> [UInt8])?
    ) -> OfflineServer.Response? {
        switch datagram.first {
        case OfflineWire.openRequestOne where useCookies:
            guard let response = server.receive(datagram, from: address) else { return nil }
            guard response.datagram.first == OfflineWire.openReplyOne else { return response }
            let cookie = cookies.issue(to: address, at: milliseconds)
            return server.receive(datagram, from: address, cookie: cookie)
        case OfflineWire.openRequestTwo where useCookies:
            guard datagram.count >= 21 else { return nil }
            let cookie = UInt32(wireBytes: datagram[17..<21])
            guard cookies.accepts(cookie, from: address, at: milliseconds) else { return nil }
            return server.receive(datagram, from: address, cookie: cookie)
        default:
            return server.receive(datagram, from: address, cookie: nil, advertisement: advertisement)
        }
    }
}
