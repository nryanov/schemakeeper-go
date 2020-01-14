package schemakeepergo

import (
	"bytes"
	"github.com/linkedin/goavro/v2"
	"testing"
)

func TestAvroSerDe_Int(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`"int"`)
	if err != nil {
		t.Error(err)
	}

	_, err = cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	serializer := CreateAvroSerializer(cfg, cachedClient)
	deserializer := CreateAvroDeserializer(cfg, cachedClient)
	data, err := serializer.Serialize("s1", 1, codec)
	if err != nil {
		t.Error(err)
	}

	record, err := deserializer.Deserialize(data)
	if err != nil {
		t.Error(err)
	}

	if record.(int32) != 1 {
		t.Error("incorrect result")
	}
}

func TestAvroSerDe_Long(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`"long"`)
	if err != nil {
		t.Error(err)
	}

	_, err = cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	serializer := CreateAvroSerializer(cfg, cachedClient)
	deserializer := CreateAvroDeserializer(cfg, cachedClient)
	data, err := serializer.Serialize("s1", int64(1), codec)
	if err != nil {
		t.Error(err)
	}

	record, err := deserializer.Deserialize(data)
	if err != nil {
		t.Error(err)
	}

	if record.(int64) != int64(1) {
		t.Error("incorrect result")
	}
}

func TestAvroSerDe_String(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`"string"`)
	if err != nil {
		t.Error(err)
	}

	_, err = cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	serializer := CreateAvroSerializer(cfg, cachedClient)
	deserializer := CreateAvroDeserializer(cfg, cachedClient)
	data, err := serializer.Serialize("s1", "test string", codec)
	if err != nil {
		t.Error(err)
	}

	record, err := deserializer.Deserialize(data)
	if err != nil {
		t.Error(err)
	}

	if record.(string) != "test string" {
		t.Error("incorrect result")
	}
}

func TestAvroSerDe_Float(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`"float"`)
	if err != nil {
		t.Error(err)
	}

	_, err = cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	serializer := CreateAvroSerializer(cfg, cachedClient)
	deserializer := CreateAvroDeserializer(cfg, cachedClient)
	data, err := serializer.Serialize("s1", float32(1), codec)
	if err != nil {
		t.Error(err)
	}

	record, err := deserializer.Deserialize(data)
	if err != nil {
		t.Error(err)
	}

	if record.(float32) != float32(1) {
		t.Error("incorrect result")
	}
}

func TestAvroSerDe_Double(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`"double"`)
	if err != nil {
		t.Error(err)
	}

	_, err = cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	serializer := CreateAvroSerializer(cfg, cachedClient)
	deserializer := CreateAvroDeserializer(cfg, cachedClient)
	data, err := serializer.Serialize("s1", float64(1), codec)
	if err != nil {
		t.Error(err)
	}

	record, err := deserializer.Deserialize(data)
	if err != nil {
		t.Error(err)
	}

	if record.(float64) != float64(1) {
		t.Error("incorrect result")
	}
}

func TestAvroSerDe_ByteArray(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`"bytes"`)
	if err != nil {
		t.Error(err)
	}

	_, err = cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	serializer := CreateAvroSerializer(cfg, cachedClient)
	deserializer := CreateAvroDeserializer(cfg, cachedClient)
	data, err := serializer.Serialize("s1", [...]byte{}, codec)
	if err != nil {
		t.Error(err)
	}

	record, err := deserializer.Deserialize(data)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(record.([]byte), []byte{}) {
		t.Error("incorrect result")
	}
}

func TestAvroSerDe_Struct(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`{"type": "record","name": "test","fields" : [{"name": "f1", "type": "long"},{"name": "f2", "type": ["null", "string"]}]}`)

	if err != nil {
		t.Error(err)
	}

	_, err = cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	serializer := CreateAvroSerializer(cfg, cachedClient)
	deserializer := CreateAvroDeserializer(cfg, cachedClient)

	test := make(map[string]interface{})
	test["f1"] = int64(1)
	test["f2"] = goavro.Union("string", "value")

	data, err := serializer.Serialize("s1", test, codec)
	if err != nil {
		t.Error(err)
	}

	record, err := deserializer.Deserialize(data)
	if err != nil {
		t.Error(err)
	}

	expected := make(map[string]interface{})
	expected["f1"] = int64(1)
	expected["f2"] = goavro.Union("string", "value")

	result := record.(map[string]interface{})

	if result["f1"].(int64) != expected["f1"].(int64) || result["f2"].(map[string]interface{})["stirng"] != expected["f2"].(map[string]interface{})["stirng"] {
		t.Error("incorrect result")
	}
}

func TestAvroSerDe_Nil(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	mockClient := CreateMockSchemaKeeperClient(cfg)
	cachedClient := CreateCachedSchemaKeeperClientUsingCustomClient(mockClient)
	codec, err := goavro.NewCodec(`"null"`)
	if err != nil {
		t.Error(err)
	}

	_, err = cachedClient.RegisterNewSchema("s1", codec, Avro, Backward)
	if err != nil {
		t.Error(err)
	}

	serializer := CreateAvroSerializer(cfg, cachedClient)
	deserializer := CreateAvroDeserializer(cfg, cachedClient)
	data, err := serializer.Serialize("s1", nil, codec)
	if err != nil {
		t.Error(err)
	}

	record, err := deserializer.Deserialize(data)
	if err != nil {
		t.Error(err)
	}

	if record != nil {
		t.Error("incorrect result")
	}
}
