import RakNet

func run(port: UInt16) async throws {
    let guid = UInt64.random(in: 1...UInt64(Int64.max))
    let advertisement = "MCPE;Bedrock Swift RakNet;0;prototype;0;0;\(guid);RakNet echo;Survival;1;\(port);0;"
    let endpoint = IPv4Endpoint(address: 0x7f000001, port: port)
    let listener = try RakNetListener.listen(on: endpoint, guid: guid, advertisement: Array(advertisement.utf8))
    defer { listener.close() }

    print("RakNet echo listening on 127.0.0.1:\(port)")
    while let connection = try await listener.accept() {
        Task {
            do {
                while let payload = try await connection.receive() {
                    try await connection.send(payload)
                }
            } catch {
                try? await connection.close()
            }
        }
    }
}

let arguments = Array(CommandLine.arguments.dropFirst())
if arguments.isEmpty {
    try await run(port: 19132)
} else if arguments.count == 1, let port = UInt16(arguments[0]), port != 0 {
    try await run(port: port)
} else if arguments.count == 2, arguments[0] == "--connect", let port = UInt16(arguments[1]), port != 0 {
    try await runEchoClient(port: port)
} else {
    print("usage: raknet-demo [port] | raknet-demo --connect port")
}
