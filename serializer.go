package schemakeepergo

import "github.com/linkedin/goavro/v2"

type Serializer interface {
	Serialize(subject string, data interface{}, schema *goavro.Codec) ([]byte, error)

	Close() error
}
