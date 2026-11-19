import Dispatch
import RakNet

private struct Peer {
    var connection: OnlineConnection
    var lastPacket: UInt64
}

func send(_ packets: [[UInt8]], to peer: IPv4Endpoint, through socket: UDPSocket) throws {
    for packet in packets {
        try socket.send(packet, to: peer)
    }
}

func run(port: UInt16) throws {
    let guid = UInt64.random(in: 1...UInt64(Int64.max))
    let advertisement = "MCPE;Bedrock Swift RakNet;0;prototype;0;0;\(guid);RakNet echo;Survival;1;\(port);0;"
    let server = try OfflineServer(guid: guid, advertisement: Array(advertisement.utf8))
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: port)
    let socket = try UDPSocket(bindTo: endpoint)
    var peers: [IPv4Endpoint: Peer] = [:]
    var buffer = [UInt8](repeating: 0, count: 65_507)
    var lastPoll: UInt64 = 0

    print("RakNet echo listening on 127.0.0.1:\(port)")
    while true {
        let datagram = try socket.receive(into: &buffer)
        let now = DispatchTime.now().uptimeNanoseconds / 1_000_000
        if let datagram {
            let sender = datagram.sender
            if let reply = server.receive(datagram.bytes, from: sender) {
                if let negotiation = reply.negotiation, peers[sender] == nil {
                    guard peers.count < 128 else { continue }
                    peers[sender] = Peer(connection: try OnlineConnection(accepting: sender, negotiation: negotiation), lastPacket: now)
                }
                try socket.send(reply.datagram, to: sender)
            } else if var peer = peers.removeValue(forKey: sender) {
                do {
                    let input = try peer.connection.receive(datagram.bytes, from: sender, at: now)
                    try send(input.replies, to: sender, through: socket)
                    for payload in input.payloads {
                        let echo = try peer.connection.send(payload, at: now)
                        try send(echo, to: sender, through: socket)
                    }
                    if !peer.connection.isClosed {
                        peer.lastPacket = now
                        peers[sender] = peer
                    }
                } catch is ConnectedSession.SessionError {
                }
            }
        }

        if now - lastPoll >= 100 {
            lastPoll = now
            for sender in Array(peers.keys) {
                guard var peer = peers.removeValue(forKey: sender), now - peer.lastPacket < 10_000 else { continue }
                do {
                    try send(try peer.connection.retransmit(at: now), to: sender, through: socket)
                    peers[sender] = peer
                } catch ConnectedSession.SessionError.retryLimit {
                }
            }
        }
    }
}

let arguments = Array(CommandLine.arguments.dropFirst())
if arguments.isEmpty {
    try run(port: 19132)
} else if arguments.count == 1, let port = UInt16(arguments[0]), port != 0 {
    try run(port: port)
} else if arguments.count == 2, arguments[0] == "--connect", let port = UInt16(arguments[1]), port != 0 {
    try runEchoClient(port: port)
} else {
    print("usage: raknet-demo [port] | raknet-demo --connect port")
}
