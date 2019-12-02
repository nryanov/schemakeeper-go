package schemakeepergo

import (
	"github.com/linkedin/goavro/v2"
)

type SchemaKeeperClient interface {
	GetSchemaById(id int32) (*goavro.Codec, error)

	RegisterNewSchema(subject string, schema *goavro.Codec, schemaType SchemaType, compatibilityType CompatibilityType) (int32, error)

	GetSchemaId(subject string, schema *goavro.Codec, schemaType SchemaType) (int32, error)

	Close() error
}
