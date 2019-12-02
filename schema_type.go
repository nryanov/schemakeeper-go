package schemakeepergo

type SchemaType string

// currently, only avro type is supported
const (
	Avro     SchemaType = "avro"
	Thrift   SchemaType = "thrift"
	Protobuf SchemaType = "protobuf"
)
