import Foundation

#if os(Windows)
import WinSDK
private typealias SocketHandle = SOCKET
private typealias AddressLength = Int32
#elseif canImport(Darwin)
import Darwin
private typealias SocketHandle = Int32
private typealias AddressLength = socklen_t
#else
import Glibc
private typealias SocketHandle = Int32
private typealias AddressLength = socklen_t
#endif

package final class UDPSocket: @unchecked Sendable {
    enum SocketError: Error {
        case startup(Int32)
        case create(Int32)
        case bind(RakNetEndpoint, Int32)
        case configure(Int32)
        case address(Int32)
        case receive(Int32)
        case send(Int32)
    }

    private let handle: SocketHandle
    private let closeLock = NSLock()
    private var closed = false
    package let localAddress: RakNetEndpoint

    package convenience init(bindTo endpoint: IPv4Endpoint, receiveTimeout: UInt32 = 100) throws {
        try self.init(bindTo: .ipv4(endpoint), receiveTimeout: receiveTimeout)
    }

    package init(bindTo endpoint: RakNetEndpoint, receiveTimeout: UInt32 = 100) throws {
        let family: Int32 = switch endpoint {
        case .ipv4: AF_INET
        case .ipv6: AF_INET6
        }
        #if os(Windows)
        var data = WSADATA()
        let startup = WSAStartup(0x0202, &data)
        guard startup == 0 else { throw SocketError.startup(startup) }
        let handle = socket(family, SOCK_DGRAM, IPPROTO_UDP.rawValue)
        let invalidHandle = SocketHandle.max
        #elseif os(Linux)
        let handle = socket(family, Int32(SOCK_DGRAM.rawValue), Int32(IPPROTO_UDP))
        let invalidHandle: SocketHandle = -1
        #else
        let handle = socket(family, SOCK_DGRAM, IPPROTO_UDP)
        let invalidHandle: SocketHandle = -1
        #endif

        guard handle != invalidHandle else {
            let error = Self.lastError
            #if os(Windows)
            _ = WSACleanup()
            #endif
            throw SocketError.create(error)
        }

        if case .ipv6 = endpoint, Self.setIPv6Only(handle) != 0 {
            let error = Self.lastError
            Self.close(handle)
            throw SocketError.configure(error)
        }

        let result = Self.withSocketAddress(for: endpoint) { bind(handle, $0, $1) }
        guard result == 0 else {
            let error = Self.lastError
            Self.close(handle)
            throw SocketError.bind(endpoint, error)
        }
        let timeoutResult: Int32 // session timer wakeup
        #if os(Windows)
        var timeout = receiveTimeout
        timeoutResult = withUnsafeBytes(of: &timeout) {
            setsockopt(handle, SOL_SOCKET, SO_RCVTIMEO, $0.baseAddress?.assumingMemoryBound(to: CChar.self), Int32($0.count))
        }
        #else
        var timeout = timeval(tv_sec: Int(receiveTimeout / 1000), tv_usec: Int(receiveTimeout % 1000) * 1000)
        timeoutResult = withUnsafeBytes(of: &timeout) {
            setsockopt(handle, SOL_SOCKET, SO_RCVTIMEO, $0.baseAddress, socklen_t($0.count))
        }
        #endif
        guard timeoutResult == 0 else {
            let error = Self.lastError
            Self.close(handle)
            throw SocketError.configure(error)
        }
        var address = sockaddr_storage()
        var boundLength = AddressLength(MemoryLayout<sockaddr_storage>.size)
        let bound = withUnsafeMutablePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { getsockname(handle, $0, &boundLength) }
        }
        guard bound == 0, let localAddress = Self.endpoint(for: &address, length: boundLength) else {
            let error = Self.lastError
            Self.close(handle)
            throw SocketError.address(error)
        }
        self.handle = handle
        self.localAddress = localAddress
    }

    convenience init(clientFor endpoint: RakNetEndpoint) throws {
        let binding: RakNetEndpoint
        switch endpoint {
        case let .ipv4(endpoint):
            let address: UInt32 = endpoint.address & 0xff000000 == 0x7f000000 ? 0x7f000001 : 0
            binding = .ipv4(IPv4Endpoint(address: address, port: 0))
        case let .ipv6(endpoint):
            let loopback = endpoint.addressHigh == 0 && endpoint.addressLow == 1
            binding = .ipv6(IPv6Endpoint(addressHigh: 0, addressLow: loopback ? 1 : 0, port: 0))
        }
        try self.init(bindTo: binding, receiveTimeout: 20)
    }

    deinit {
        close()
    }

    func close() {
        closeLock.withLock {
            guard !closed else { return }
            closed = true
            Self.close(handle)
        }
    }

    package func receive(into buffer: inout [UInt8]) throws -> (bytes: [UInt8], sender: RakNetEndpoint)? {
        var address = sockaddr_storage()
        var size = AddressLength(MemoryLayout<sockaddr_storage>.size)
        let count = withUnsafeMutablePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { address in
                buffer.withUnsafeMutableBytes { bytes in
                    #if os(Windows)
                    return Int(recvfrom(
                        handle, bytes.baseAddress?.assumingMemoryBound(to: CChar.self),
                        Int32(bytes.count), 0, address, &size
                    ))
                    #else
                    return recvfrom(handle, bytes.baseAddress, bytes.count, 0, address, &size)
                    #endif
                }
            }
        }

        if count < 0 {
            let error = Self.lastError
            #if os(Windows)
            if error == WSAEINTR || error == WSAECONNRESET || error == WSAETIMEDOUT { return nil }
            #else
            if error == EINTR || error == EAGAIN || error == EWOULDBLOCK { return nil }
            #endif
            throw SocketError.receive(error)
        }

        guard let sender = Self.endpoint(for: &address, length: size) else { return nil }
        return (Array(buffer.prefix(count)), sender)
    }

    package func send(_ datagram: [UInt8], to endpoint: RakNetEndpoint) throws {
        let count = Self.withSocketAddress(for: endpoint) { address, size in
            datagram.withUnsafeBytes { bytes in
                #if os(Windows)
                return Int(sendto(
                    handle, bytes.baseAddress?.assumingMemoryBound(to: CChar.self),
                    Int32(bytes.count), 0, address, size
                ))
                #else
                return sendto(handle, bytes.baseAddress, bytes.count, 0, address, size)
                #endif
            }
        }
        if count < 0 {
            let error = Self.lastError
            #if os(Windows)
            if error == WSAEINTR || error == WSAECONNRESET { return }
            #else
            if error == EINTR { return }
            #endif
            throw SocketError.send(error)
        }
    }

    static func resolve(_ host: String, port: UInt16) throws -> [RakNetEndpoint] {
        #if os(Windows)
        var data = WSADATA()
        let startup = WSAStartup(0x0202, &data)
        guard startup == 0 else { throw RakNetEndpoint.ResolutionError.lookupFailed(startup) }
        defer { _ = WSACleanup() }
        #endif

        var hints = addrinfo()
        hints.ai_family = AF_UNSPEC
        #if os(Linux)
        hints.ai_socktype = Int32(SOCK_DGRAM.rawValue)
        #else
        hints.ai_socktype = SOCK_DGRAM
        #endif
        var list: UnsafeMutablePointer<addrinfo>?
        let error = getaddrinfo(host, String(port), &hints, &list)
        guard error == 0 else { throw RakNetEndpoint.ResolutionError.lookupFailed(error) }
        guard let list else { throw RakNetEndpoint.ResolutionError.noAddresses }
        defer { freeaddrinfo(list) }

        var results: [RakNetEndpoint] = []
        var unique: Set<RakNetEndpoint> = []
        var next: UnsafeMutablePointer<addrinfo>? = list
        while let entry = next?.pointee {
            if let address = entry.ai_addr, entry.ai_addrlen <= MemoryLayout<sockaddr_storage>.size,
               let endpoint = endpoint(for: UnsafePointer(address), length: AddressLength(entry.ai_addrlen)),
               unique.insert(endpoint).inserted {
                results.append(endpoint)
            }
            next = entry.ai_next
        }
        guard !results.isEmpty else { throw RakNetEndpoint.ResolutionError.noAddresses }
        return results
    }

    private static func endpoint(for storage: inout sockaddr_storage, length: AddressLength) -> RakNetEndpoint? {
        withUnsafePointer(to: &storage) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { endpoint(for: $0, length: length) }
        }
    }

    private static func endpoint(for address: UnsafePointer<sockaddr>, length: AddressLength) -> RakNetEndpoint? {
        switch Int32(address.pointee.sa_family) {
        case AF_INET where length >= MemoryLayout<sockaddr_in>.size:
            return address.withMemoryRebound(to: sockaddr_in.self, capacity: 1) {
                let address = $0.pointee
                return .ipv4(IPv4Endpoint(
                    address: UInt32(bigEndian: address.sin_addr.networkAddress),
                    port: UInt16(bigEndian: address.sin_port)
                ))
            }
        case AF_INET6 where length >= MemoryLayout<sockaddr_in6>.size:
            return address.withMemoryRebound(to: sockaddr_in6.self, capacity: 1) {
                let address = $0.pointee
                return withUnsafeBytes(of: address.sin6_addr) { bytes in
                    .ipv6(IPv6Endpoint(
                        addressHigh: UInt64(wireBytes: bytes.prefix(8)),
                        addressLow: UInt64(wireBytes: bytes.dropFirst(8).prefix(8)),
                        port: UInt16(bigEndian: address.sin6_port),
                        scopeID: address.sin6_scope_id
                    ))
                }
            }
        default:
            return nil
        }
    }

    private static func withSocketAddress<T>(
        for endpoint: RakNetEndpoint, _ operation: (UnsafePointer<sockaddr>, AddressLength) -> T
    ) -> T {
        switch endpoint {
        case let .ipv4(endpoint):
            var address = sockaddr_in()
            #if canImport(Darwin)
            address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
            #endif
            address.sin_family = numericCast(AF_INET)
            address.sin_port = endpoint.port.bigEndian
            address.sin_addr.networkAddress = endpoint.address.bigEndian
            return withUnsafePointer(to: &address) { pointer in
                pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    operation($0, AddressLength(MemoryLayout<sockaddr_in>.size))
                }
            }
        case let .ipv6(endpoint):
            var address = sockaddr_in6()
            #if canImport(Darwin)
            address.sin6_len = UInt8(MemoryLayout<sockaddr_in6>.size)
            #endif
            address.sin6_family = numericCast(AF_INET6)
            address.sin6_port = endpoint.port.bigEndian
            address.sin6_scope_id = endpoint.scopeID
            withUnsafeMutableBytes(of: &address.sin6_addr) {
                $0.copyBytes(from: bigEndianBytes(endpoint.addressHigh) + bigEndianBytes(endpoint.addressLow))
            }
            return withUnsafePointer(to: &address) { pointer in
                pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    operation($0, AddressLength(MemoryLayout<sockaddr_in6>.size))
                }
            }
        }
    }

    private static func setIPv6Only(_ handle: SocketHandle) -> Int32 {
        var enabled: Int32 = 1
        return withUnsafeBytes(of: &enabled) {
            #if os(Windows)
            setsockopt(handle, IPPROTO_IPV6.rawValue, IPV6_V6ONLY, $0.baseAddress?.assumingMemoryBound(to: CChar.self), Int32($0.count))
            #else
            setsockopt(handle, Int32(IPPROTO_IPV6), IPV6_V6ONLY, $0.baseAddress, socklen_t($0.count))
            #endif
        }
    }

    private static var lastError: Int32 {
        #if os(Windows)
        WSAGetLastError()
        #else
        errno
        #endif
    }

    private static func close(_ handle: SocketHandle) {
        #if os(Windows)
        _ = closesocket(handle)
        _ = WSACleanup()
        #elseif canImport(Darwin)
        _ = Darwin.close(handle)
        #else
        _ = Glibc.close(handle)
        #endif
    }
}

private extension in_addr {
    var networkAddress: UInt32 {
        get {
            #if os(Windows)
            S_un.S_addr
            #else
            s_addr
            #endif
        }
        set {
            #if os(Windows)
            S_un.S_addr = newValue
            #else
            s_addr = newValue
            #endif
        }
    }
}
