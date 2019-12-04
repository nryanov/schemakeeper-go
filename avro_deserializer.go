package schemakeepergo

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type avroDeserializer struct {
	Configuration Configuration
	Client        SchemaKeeperClient
}

func (a *avroDeserializer) Deserialize(data []byte) (interface{}, error) {
	if data == nil {
		return nil, nil
	}

	buffer := bytes.NewBuffer(data)
	schemaByteType, err := buffer.ReadByte()
	if err != nil {
		return nil, err
	}

	err = CheckByte(schemaByteType)
	if err != nil {
		return nil, err
	}

	schemaIdByteBuffer := make([]byte, 4)
	readBytes, err := buffer.Read(schemaIdByteBuffer)
	if err != nil {
		return nil, err
	}
	if readBytes != 4 {
		return nil, errors.New("incorrect data")
	}

	schemaId := binary.LittleEndian.Uint32(schemaIdByteBuffer)
	schema, err := a.Client.GetSchemaById(int32(schemaId))
	if err != nil {
		return nil, err
	}

	record, remained, err := schema.NativeFromBinary(data[5:])
	if err != nil {
		return nil, err
	}

	if len(remained) != 0 {
		return nil, errors.New("incorrect data")
	}

	return record, nil
}

func (a *avroDeserializer) Close() error {
	return a.Client.Close()
}

func CreateAvroDeserializer(cfg Configuration, client SchemaKeeperClient) Deserializer {
	return &avroDeserializer{
		Configuration: cfg,
		Client:        client,
	}
}
