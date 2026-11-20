<div align="center">

# RakNet

A RakNet transport library for Minecraft: Bedrock Edition, written in Swift.

[![Swift 6](https://img.shields.io/badge/Swift-6-orange?style=flat-square&logo=swift)](https://www.swift.org)
[![RakNet transport](https://img.shields.io/badge/transport-RakNet-black?style=flat-square)](#features)
[![MIT license](https://img.shields.io/github/license/bedrock-swift/raknet?style=flat-square)](LICENSE)

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

Use `listener.block(_:for:)` and `listener.unblock(_:)` to control access by IP address. Set `offlinePacketLimits` to adjust the per-IP and listener-wide rates for discovery and offline handshakes. Set `pendingConnections` to cap unfinished handshakes separately. The defaults allow 64 pending handshakes in total and eight per IP. Set `maximumConnectionsPerIP` to limit established connections from one address and `onlinePacketsPerSecond` to cap the packet rate of each connection.

Read `connection.roundTripTime` for the smoothed RTT once a measurement is available.

A send can report `sendQueueFull` when pending commands reach a transport limit or `sendWindowFull` when the peer’s reliable window is full. In either case the batch can be retried after the queue drains or acknowledgements arrive.

Send defaults to reliable ordered delivery and immediate priority. Pass `priority` to schedule high, normal, or low priority sends. Ordered delivery is still preserved within each channel. Pass `reliability` and `channel` to select a different mode. Send several payloads with `try await connection.sendBatch(payloads)` to pack their frames into datagrams up to the negotiated MTU. Unreliable payloads that require fragmentation use the corresponding reliable mode.

Use `RakNetEndpoint.ipv6(IPv6Endpoint(addressHigh: 0, addressLow: 1, port: 19132))` for IPv6 loopback.

Both sides use RakNet protocol 11 by default. Set `supportedProtocols` on the listener or `protocolVersion` on the dialer to configure the handshake.

The default MTU is 1440 bytes. Use `maximumMTU` on the listener or `mtu` on the dialer to set a different value. `OfflineClient`, `OfflineServerHandshake`, and `OnlineConnection` expose the protocol state machines for applications that provide their own UDP I/O. Use `RakNetServer` to manage multiple peers on the same transport. Call `update(at:)` periodically to send pending acknowledgements, retries, and queued payloads.

Use `try await connection.snapshot()` to capture session state for custom I/O. Use `snapshot.encoded()` and `RakNetSessionSnapshot(data:)` to persist it. It includes unread payloads and the monotonic time in milliseconds at capture. Call `snapshot.restore(at:)` to resume on a different monotonic clock. Use `connection.detach()` and `listener.adopt(snapshot)` to hand off a live server session on the same local address. Use `RakNetConnection.resume(snapshot)` to reopen a detached client.

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
