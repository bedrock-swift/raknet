<div align="center">

# RakNet

RakNet transport library for Minecraft Bedrock Edition, written in Swift.

[![Swift 6](https://img.shields.io/badge/Swift-6-orange?style=flat-square&logo=swift)](https://www.swift.org)

</div>

## Features

The prototype supports IPv4 discovery, RakNet 11 handshakes, and bidirectional data transfer. Sessions use reliable ordered delivery, ACK/NACK, retransmission, and fragment reassembly.

The client supports MTU negotiation and server cookies. Session timers take monotonic timestamps in milliseconds.

## Usage

`RakNet` processes datagram bytes and leaves UDP I/O to the caller. Pass the sender address observed by the socket and send any returned reply to that address.

```swift
import RakNet

let server = try OfflineServer(guid: 42, advertisement: Array("Example".utf8))
let sender = IPv4Endpoint(address: 0x7f000001, port: 49152)
func response(to datagram: [UInt8]) -> [UInt8]? {
    server.reply(to: datagram, from: sender)
}
```

`IPv4Endpoint.address` uses the numeric IPv4 address in host byte order. The advertisement contains application-defined bytes.

The default IPv4 MTU is 1440 bytes, leaving 1412 bytes for UDP data. Set the server cap with `maximumMTU` and the client probe size with `probe(mtu:)`.

## Development

Build with [Swift 6.0 or later](https://www.swift.org/install/). On Windows, follow the [Swift installation requirements](https://www.swift.org/install/windows/manual/).

```sh
swift test
swift run raknet-demo 19132
```

The demo listens on UDP `127.0.0.1:19132` and echoes application packets. To send a 4096-byte message, run the client in another terminal.

```sh
swift run raknet-demo --connect 19132
```

The protocol module has no package dependencies.
