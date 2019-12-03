package schemakeepergo

import (
	"github.com/linkedin/goavro/v2"
	"golang.org/x/sync/syncmap"
	"testing"
)

func TestCachedSchemaKeeperClient_GetSchemaById(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`"string"`)
	if err != nil {
		t.Error(err)
	}

	_, err = cachedClient.GetSchemaId("s1", codec, Avro)
	if err == nil {
		t.Error("Unexpected result")
	}

	id, err := cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	returnedId, err := cachedClient.GetSchemaId("s1", codec, Avro)
	if err != nil {
		t.Error(err)
	}

	if id != returnedId {
		t.Error("Incorrect returned id")
	}

	if mockClient.(*MockSchemaKeeperClient).GetSchemaIdCallsCount != 1 {
		t.Error("GetSchemaId should be called once")
	}
}

func TestCachedSchemaKeeperClient_RegisterNewSchema(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`"string"`)
	if err != nil {
		t.Error(err)
	}

	firstId, err := cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	secondId, err := cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	if firstId != secondId {
		t.Error("Incorrect returned id")
	}

	if mockClient.(*MockSchemaKeeperClient).RegisterNewSchemaCallsCount != 1 {
		t.Error("RegisterNewSchema should be called once")
	}
}

func TestCachedSchemaKeeperClient_GetSchemaId(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`"string"`)
	if err != nil {
		t.Error(err)
	}

	schemas, _ := mockClient.(*MockSchemaKeeperClient).SubjectSchemas.LoadOrStore("s1", &syncmap.Map{})
	schemas.(*syncmap.Map).Store(codec.Schema(), int32(1))

	_, err = cachedClient.GetSchemaId("s1", codec, Avro)
	if err != nil {
		t.Error(err)
	}

	_, err = cachedClient.GetSchemaId("s1", codec, Avro)
	if err != nil {
		t.Error(err)
	}

	if mockClient.(*MockSchemaKeeperClient).GetSchemaIdCallsCount != 1 {
		t.Error("GetSchemaId should be called once: $d", mockClient.(*MockSchemaKeeperClient).GetSchemaIdCallsCount)
	}
}
