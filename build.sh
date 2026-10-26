#!/usr/bin/env bash
# Compileaza ProxyHub pentru Windows si Linux in ./bin
set -e
cd "$(dirname "$0")"
mkdir -p bin
LD="-s -w"
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "$LD" -o bin/proxyhub-windows-amd64.exe .
GOOS=linux   GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "$LD" -o bin/proxyhub-linux-amd64 .
GOOS=linux   GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags "$LD" -o bin/proxyhub-linux-arm64 .
ls -la bin
