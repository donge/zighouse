# Install

## macOS (Homebrew)

```bash
brew tap donge/zighouse
brew install zighouse
brew services start zighouse
```

## Linux / Docker

```bash
docker pull ghcr.io/donge/zighouse:latest
```

Binary releases are available on the
[releases page](https://github.com/donge/zighouse/releases).

## From Source

Requires [Zig 0.16+](https://ziglang.org/download/).

```bash
git clone https://github.com/donge/zighouse.git
cd zighouse
zig build -Doptimize=ReleaseFast -Dstrip=true -Dstatic-libs=true
cp zig-out/bin/zighouse /usr/local/bin/
```

## Verify

```bash
zighouse --version
zighouse --help
```
