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

package final class UDPSocket: Sendable {
    enum SocketError: Error {
        case startup(Int32)
        case create(Int32)
        case bind(IPv4Endpoint, Int32)
        case configure(Int32)
        case address(Int32)
        case receive(Int32)
        case send(Int32)
    }

    private let handle: SocketHandle
    package let localAddress: IPv4Endpoint

    package init(bindTo endpoint: IPv4Endpoint) throws {
        #if os(Windows)
        var data = WSADATA()
        let startup = WSAStartup(0x0202, &data)
        guard startup == 0 else { throw SocketError.startup(startup) }
        let handle = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP.rawValue)
        let invalidHandle = SocketHandle.max
        #elseif os(Linux)
        let handle = socket(AF_INET, Int32(SOCK_DGRAM.rawValue), Int32(IPPROTO_UDP))
        let invalidHandle: SocketHandle = -1
        #else
        let handle = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
        let invalidHandle: SocketHandle = -1
        #endif

        guard handle != invalidHandle else {
            let error = Self.lastError
            #if os(Windows)
            _ = WSACleanup()
            #endif
            throw SocketError.create(error)
        }

        var address = Self.socketAddress(for: endpoint)
        let size = AddressLength(MemoryLayout<sockaddr_in>.size)
        let result = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(handle, $0, size) }
        }
        guard result == 0 else {
            let error = Self.lastError
            Self.close(handle)
            throw SocketError.bind(endpoint, error)
        }
        let timeoutResult: Int32 // session timer wakeup
        #if os(Windows)
        var timeout: UInt32 = 100
        timeoutResult = withUnsafeBytes(of: &timeout) {
            setsockopt(handle, SOL_SOCKET, SO_RCVTIMEO, $0.baseAddress?.assumingMemoryBound(to: CChar.self), Int32($0.count))
        }
        #else
        var timeout = timeval(tv_sec: 0, tv_usec: 100_000)
        timeoutResult = withUnsafeBytes(of: &timeout) {
            setsockopt(handle, SOL_SOCKET, SO_RCVTIMEO, $0.baseAddress, socklen_t($0.count))
        }
        #endif
        guard timeoutResult == 0 else {
            let error = Self.lastError
            Self.close(handle)
            throw SocketError.configure(error)
        }
        var boundLength = size
        let bound = withUnsafeMutablePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { getsockname(handle, $0, &boundLength) }
        }
        guard bound == 0 else {
            let error = Self.lastError
            Self.close(handle)
            throw SocketError.address(error)
        }
        self.handle = handle
        localAddress = Self.endpoint(for: address)
    }

    deinit {
        Self.close(handle)
    }

    package func receive(into buffer: inout [UInt8]) throws -> (bytes: [UInt8], sender: IPv4Endpoint)? {
        var address = sockaddr_in()
        var size = AddressLength(MemoryLayout<sockaddr_in>.size)
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

        return (Array(buffer.prefix(count)), Self.endpoint(for: address))
    }

    package func send(_ datagram: [UInt8], to endpoint: IPv4Endpoint) throws {
        var address = Self.socketAddress(for: endpoint)
        let size = AddressLength(MemoryLayout<sockaddr_in>.size)
        let count = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { address in
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

    private static func endpoint(for address: sockaddr_in) -> IPv4Endpoint {
        IPv4Endpoint(
            address: UInt32(bigEndian: address.sin_addr.networkAddress),
            port: UInt16(bigEndian: address.sin_port)
        )
    }

    private static func socketAddress(for endpoint: IPv4Endpoint) -> sockaddr_in {
        var address = sockaddr_in()
        #if canImport(Darwin)
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        #endif
        address.sin_family = UInt16(AF_INET)
        address.sin_port = endpoint.port.bigEndian
        address.sin_addr.networkAddress = endpoint.address.bigEndian
        return address
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
