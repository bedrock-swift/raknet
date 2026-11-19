// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "raknet",
    products: [
        .library(name: "RakNet", targets: ["RakNet"]),
        .executable(name: "raknet-demo", targets: ["RakNetDemo"])
    ],
    targets: [
        .target(name: "RakNet"),
        .executableTarget(
            name: "RakNetDemo",
            dependencies: ["RakNet"],
            linkerSettings: [.linkedLibrary("ws2_32", .when(platforms: [.windows]))]
        ),
        .testTarget(name: "RakNetTests", dependencies: ["RakNet"])
    ]
)
