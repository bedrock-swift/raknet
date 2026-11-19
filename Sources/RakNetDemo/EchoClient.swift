import Dispatch
import RakNet

enum EchoClientError: Error {
    case timeout
    case peerClosed
    case unexpectedPayload
}

func runEchoClient(port: UInt16) throws {
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: port)
    let socket = try UDPSocket(port: 0)
    var offline = OfflineClient(server: endpoint)
    var connection: OnlineConnection?
    let probe = try offline.probe()
    let payload = (0..<4096).map { UInt8(truncatingIfNeeded: $0 * 19 + 0xfe) }
    let started = DispatchTime.now().uptimeNanoseconds / 1_000_000
    var nextProbe = started

    while true {
        let now = DispatchTime.now().uptimeNanoseconds / 1_000_000
        guard now - started < 5_000 else { throw EchoClientError.timeout }

        if var online = connection {
            try send(try online.retransmit(at: now), to: endpoint, through: socket)
            connection = online
        } else if now >= nextProbe {
            try socket.send(offline.pendingOpenRequest ?? probe, to: endpoint)
            nextProbe = now + 350
        }

        guard let datagram = try socket.receive() else { continue }
        if var online = connection {
            let input = try online.receive(datagram.bytes, from: datagram.sender, at: now)
            try send(input.replies, to: endpoint, through: socket)
            if input.closed { throw EchoClientError.peerClosed }
            if input.opened {
                try send(try online.send(payload, at: now), to: endpoint, through: socket)
            }
            if let echo = input.payloads.first {
                guard echo == payload else { throw EchoClientError.unexpectedPayload }
                try send(try online.close(at: now), to: endpoint, through: socket)
                print("Received \(echo.count) echoed bytes")
                return
            }
            connection = online
        } else {
            switch try offline.receive(datagram.bytes, from: datagram.sender) {
            case let .send(request):
                try socket.send(request, to: endpoint)
                nextProbe = now + 350
            case let .negotiated(negotiation):
                var online = try OnlineConnection(connectingTo: endpoint, mtu: negotiation.mtu, clientGUID: offline.guid)
                try send(try online.start(at: now), to: endpoint, through: socket)
                connection = online
            case nil:
                break
            }
        }
    }
}
