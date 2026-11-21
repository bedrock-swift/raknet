<div align="center">

# RakNet

A RakNet transport library for Minecraft: Bedrock Edition, written in Swift.

[![Swift 6.0+](https://img.shields.io/badge/Swift-6.0%2B-orange?style=flat-square&logo=swift)](https://www.swift.org)
[![RakNet transport](https://img.shields.io/badge/transport-RakNet-black?style=flat-square)](https://github.com/facebookarchive/RakNet)
[![MIT license](https://img.shields.io/github/license/bedrock-swift/raknet?style=flat-square)](LICENSE)

</div>

## Getting started

Add RakNet to your Swift package and use the `RakNet` product. Swift 6.0 or later is required.

```swift
.package(url: "https://github.com/bedrock-swift/raknet.git", branch: "main")
```

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
try await connection.send([0x01])
print(try await connection.receive() ?? [])
try await connection.close()
```

Use `RakNetConnection.ping(_:)` to query the server advertisement without connecting. A reconnect can replace a session on the same endpoint after a fresh cookie exchange. With `useCookies: false`, the existing session must close first.

## Development

```sh
swift test
```

See [Contributing](.github/CONTRIBUTING.md).
