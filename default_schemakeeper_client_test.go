package schemakeepergo

import (
	"github.com/linkedin/goavro/v2"
	"testing"
)

func TestDefaultSchemaKeeperClient(t *testing.T) {
	codec, err := goavro.NewCodec(`"string"`)
	if err != nil {
		t.Error(err)
	}

	cfg := CreateConfiguration("http://192.168.99.100:9081")
	client := CreateDefaultSchemaKeeperClient(cfg)
	_, err = client.GetSchemaById(1)

	if err == nil {
		t.Errorf("GetSchemaById returned unexpected result")
	}

	id, err := client.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	schemaId, err := client.GetSchemaId("s1", codec, Avro)
	if err != nil {
		t.Error(err)
	}

	if id != schemaId {
		t.Errorf("Returned schema id is not equal to id returned after schema registration")
	}

	schema, err := client.GetSchemaById(id)
	if err != nil {
		t.Error(err)
	}

	if schema.Schema() != codec.Schema() {
		t.Errorf("Returned schema is not equal to schema used in registration")
	}
}