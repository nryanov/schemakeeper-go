package schemakeepergo

type Deserializer interface {
	Deserialize(data []byte) (interface{}, error)

	Close() error
}
