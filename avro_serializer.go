package schemakeepergo

import (
	"bytes"
	"encoding/binary"
	"github.com/linkedin/goavro/v2"
)

type avroSerializer struct {
	Configuration Configuration
	Client        SchemaKeeperClient
}

func (a *avroSerializer) Serialize(subject string, data interface{}, schema *goavro.Codec) ([]byte, error) {
	id, err := a.getSchemaId(subject, schema)
	if err != nil {
		return nil, err
	}

	buffer := &bytes.Buffer{}
	err = buffer.WriteByte(AvroByte)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buffer, binary.LittleEndian, id)
	if err != nil {
		return nil, err
	}

	binaryData, ok := data.([]byte)
	if ok {
		_, err = buffer.Write(binaryData)
		if err != nil {
			return nil, err
		}

		return buffer.Bytes(), nil
	}

	binaryData, err = schema.BinaryFromNative(nil, data)
	if err != nil {
		return nil, err
	}

	_, err = buffer.Write(binaryData)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (a *avroSerializer) getSchemaId(subject string, schema *goavro.Codec) (int32, error) {
	if a.Configuration.IsForceSchemaRegistrationAllowed() {
		return a.Client.RegisterNewSchema(subject, schema, Avro, a.Configuration.GetDefaultCompatibilityType())
	} else {
		return a.Client.GetSchemaId(subject, schema, Avro)
	}
}

func (a *avroSerializer) Close() error {
	return a.Client.Close()
}

func CreateAvroSerializer(cfg Configuration, client SchemaKeeperClient) Serializer {
	return &avroSerializer{
		Configuration: cfg,
		Client:        client,
	}
}
