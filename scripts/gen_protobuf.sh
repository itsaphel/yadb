#!/bin/zsh

export PATH="$PATH:$(go env GOPATH)/bin"
protoc --proto_path=protobuf --go_out=protoc --go_opt=paths=source_relative protobuf/*.proto