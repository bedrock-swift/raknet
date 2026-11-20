# Contributing to RakNet

## Issues

Open an issue for a bug or a feature request. For a bug, include the Swift version, operating system, and the smallest steps that reproduce it. For interoperability issues, name the peer implementation and version, and include the packet IDs or a short trace if available.

For a feature request, describe the use case and the behavior you need. Discuss changes to the wire protocol or public API in an issue before opening a large pull request.

## Pull requests

Keep each pull request focused on one change. Explain what it changes and how you verified it.

- Build with Swift 6 and run `swift test`.
- Add focused coverage for changes to the wire format or session behavior.
- Use the [echo demo](../README.md#development) to check changes to UDP transport.
- Follow the surrounding Swift style. Keep source comments short, lowercase, and beside the relevant code.
