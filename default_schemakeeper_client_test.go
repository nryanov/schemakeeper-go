package schemakeepergo

import (
	"github.com/linkedin/goavro/v2"
	"github.com/ory/dockertest/v3"
	"log"
	"testing"
)

func TestDefaultSchemaKeeperClient(t *testing.T) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	options := &dockertest.RunOptions{
		Repository: "schemakeeper/server",
		Tag:        "0.1",
		ExposedPorts: []string{"9081"},
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	var host string

	err = pool.Retry(func() error {
		host = resource.GetHostPort("9081/tcp")
		return nil
	})

	if err != nil {
		t.Error(err)
	}

	codec, err := goavro.NewCodec(`"string"`)
	if err != nil {
		t.Error(err)
	}

	cfg := CreateConfiguration("http://" + host)
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

	err = pool.Purge(resource)
	if err != nil {
		t.Error(err)
	}
}