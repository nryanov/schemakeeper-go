# Schemakeeper-go

[![Build Status](https://img.shields.io/travis/nryanov/schemakeeper-go/master.svg)](https://travis-ci.com/nryanov/schemakeeper-go)
[![Coverage Status](https://coveralls.io/repos/github/nryanov/schemakeeper-go/badge.svg?branch=master)](https://coveralls.io/github/nryanov/schemakeeper-go?branch=master)
[![GitHub license](https://img.shields.io/github/license/nryanov/schemakeeper-go)](https://github.com/nryanov/schemakeeper-go/blob/master/LICENSE.txt)

This is a golang-client for [schemakeeper](https://github.com/nryanov/schemakeeper).

## Install 
`go dep github.com/nryanov/schemakeeper-go 0.2.0`

## Usage
```go
package main

import (
    "fmt"
    "github.com/linkedin/goavro/v2"
    "github.com/nryanov/schemakeeper-go"
)

func main() {
    codec, err := goavro.NewCodec(`{"type": "record","name": "test","fields" : [{"name": "f1", "type": "long"},{"name": "f2", "type": ["null", "string"]}]}`)
    if err != nil {
        fmt.Println(err)
    }

	cfg := schemakeepergo.CreateConfiguration("host:port")
	client := schemakeepergo.CreateCachedSchemaKeeperClient(cfg)

	serializer := schemakeepergo.CreateAvroSerializer(cfg, client)
	deserializer := schemakeepergo.CreateAvroDeserializer(cfg, client)

	value := make(map[string]interface{})
	value["f1"] = int64(1)
	value["f2"] = goavro.Union("string", "value")
	
    data, err := serializer.Serialize("subjectName", value, codec)
    if err != nil {
        fmt.Println(err)
    }

	record, err := deserializer.Deserialize(data)
	if err != nil {
		fmt.Println(err)
	}

    fmt.Println(record)
}
```

## Limitations
Currently, this client supports only avro serialization/deserialization. 
