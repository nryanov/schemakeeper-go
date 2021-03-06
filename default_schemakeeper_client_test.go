package schemakeepergo

import (
	"fmt"
	"github.com/linkedin/goavro/v2"
	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"log"
	"testing"
	"time"
)

func TestDefaultSchemaKeeperClient(t *testing.T) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	options := &dockertest.RunOptions{
		Repository: "nryanov/schemakeeper",
		Tag:        "0.2.0",
		ExposedPorts: []string{"9081"},
		PortBindings: map[dc.Port][]dc.PortBinding{
			"9081": {{HostPort: "9081"}},
		},
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	var port string

	err = pool.Retry(func() error {
		port = resource.GetPort("9081/tcp")
		return nil
	})

	if err != nil {
		t.Error(err)
	}

	time.Sleep(15 * time.Second)

	codec, err := goavro.NewCodec(`"string"`)
	if err != nil {
		t.Error(err)
	}

	cfg := CreateConfiguration(fmt.Sprintf("http://localhost:%s", port))
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