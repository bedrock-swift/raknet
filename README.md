<div align="center">

# RakNet

A RakNet transport library for Minecraft: Bedrock Edition, written in Swift.

[![Swift 6](https://img.shields.io/badge/Swift-6-orange?style=flat-square&logo=swift)](https://www.swift.org)
[![RakNet transport](https://img.shields.io/badge/transport-RakNet-black?style=flat-square)](#features)

</div>

## Features

- IPv4 discovery and RakNet 11 handshakes
- Reliable, unreliable, ordered, and sequenced delivery
- ACK/NACK, retransmission, and fragment reassembly
- MTU negotiation and server cookie validation
- Async clients and listeners backed by system UDP sockets

## Usage

### Listen

```swift
import RakNet

let address = IPv4Endpoint(address: 0x7f000001, port: 19132)
let listener = try RakNetListener.listen(on: address)
defer { listener.close() }

if let connection = try await listener.accept() {
    while let packet = try await connection.receive() {
        try await connection.send(packet)
    }
}
```

### Dial

```swift
import RakNet

let address = IPv4Endpoint(address: 0x7f000001, port: 19132)
let connection = try await RakNetConnection.dial(address)
try await connection.send([0xfe, 0x01])
if let reply = try await connection.receive() {
    print(reply)
}
try await connection.close()
```

Send defaults to reliable ordered delivery. Pass `reliability` and `channel` to select a different mode.

The default IPv4 MTU is 1440 bytes. Use `maximumMTU` on the listener or `mtu` on the dialer to set a different value. `OnlineConnection` exposes the protocol state machine for applications that provide their own UDP I/O.

## Development

Build with [Swift 6.0 or later](https://www.swift.org/install/). On Windows, follow the [Swift installation requirements](https://www.swift.org/install/windows/manual/).

```sh
swift test
swift run raknet-demo 19132
```

Run the client in a second terminal to send a 4096-byte message to the loopback echo server.

```sh
swift run raknet-demo --connect 19132
```
