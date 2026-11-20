<div align="center">

# RakNet

A RakNet transport library for Minecraft: Bedrock Edition, written in Swift.

[![Swift 6](https://img.shields.io/badge/Swift-6-orange?style=flat-square&logo=swift)](https://www.swift.org)
[![RakNet transport](https://img.shields.io/badge/transport-RakNet-black?style=flat-square)](#features)

</div>

## Features

- IPv4 and IPv6 discovery and RakNet 11 handshakes
- Reliable, unreliable, ordered, and sequenced delivery
- ACK/NACK, retransmission, congestion control, and fragment reassembly
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

Use `try await RakNetConnection.ping(address)` to read the server advertisement and round-trip time. Both `ping` and `dial` also accept a hostname or IP literal and a `port`.
Use `listener.setAdvertisement(_:)` to update the advertisement or `listener.setAdvertisementProvider(_:)` to generate it per peer. If the provider returns an oversized advertisement, the listener uses the configured static bytes.

Use `listener.block(_:for:)` and `listener.unblock(_:)` to control access by IP address. Set `offlinePacketLimits` to adjust the per-IP and listener-wide rates for discovery and offline handshakes. Set `maximumConnectionsPerIP` to limit concurrent connections from one address.

Read `connection.roundTripTime` for the smoothed RTT once a measurement is available.

Send defaults to reliable ordered delivery and immediate priority. Pass `priority` to schedule high, normal, or low priority sends. Ordered delivery is still preserved within each channel. Pass `reliability` and `channel` to select a different mode. Send several payloads with `try await connection.sendBatch(payloads)` to pack their frames into datagrams up to the negotiated MTU.

Use `RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 1, port: 19132))` for IPv6 loopback.

The default MTU is 1440 bytes. Use `maximumMTU` on the listener or `mtu` on the dialer to set a different value. `OnlineConnection` exposes the protocol state machine for applications that provide their own UDP I/O. Call `update(at:)` periodically to send pending acknowledgements, retries, and queued payloads.

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
