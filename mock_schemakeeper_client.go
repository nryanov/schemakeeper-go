package schemakeepergo

import (
	"errors"
	"github.com/linkedin/goavro/v2"
	"golang.org/x/sync/syncmap"
)

type MockSchemaKeeperClient struct {
	Configuration               Configuration
	GetSchemaByIdCallsCount     uint32
	RegisterNewSchemaCallsCount uint32
	GetSchemaIdCallsCount       uint32
	UniqueId                    int32
	SubjectSchemas              *syncmap.Map
	IdToSchema                  *syncmap.Map
}

func (d *MockSchemaKeeperClient) GetSchemaById(id int32) (*goavro.Codec, error) {
	d.GetSchemaByIdCallsCount++
	schema, exist := d.IdToSchema.Load(id)
	if !exist {
		return nil, errors.New("not exist")
	}
	return schema.(*goavro.Codec), nil
}

func (d *MockSchemaKeeperClient) RegisterNewSchema(subject string, schema *goavro.Codec, schemaType SchemaType, compatibilityType CompatibilityType) (int32, error) {
	d.RegisterNewSchemaCallsCount++
	schemas, _ := d.SubjectSchemas.LoadOrStore(subject, &syncmap.Map{})

	id, exist := schemas.(*syncmap.Map).Load(schema.Schema())
	if !exist {
		d.UniqueId++
		id = d.UniqueId
		schemas.(*syncmap.Map).Store(schema.Schema(), id)
		d.IdToSchema.Store(id, schema)
		return d.UniqueId, nil
	} else {
		return id.(int32), nil
	}
}

func (d *MockSchemaKeeperClient) GetSchemaId(subject string, schema *goavro.Codec, schemaType SchemaType) (int32, error) {
	d.GetSchemaIdCallsCount++
	schemas, _ := d.SubjectSchemas.LoadOrStore(subject, &syncmap.Map{})

	id, exist := schemas.(*syncmap.Map).Load(schema.Schema())
	if !exist {
		return -1, errors.New("not exist")
	} else {
		return id.(int32), nil
	}
}

func (d *MockSchemaKeeperClient) Close() error {
	return nil
}

func CreateMockSchemaKeeperClient(cfg Configuration) SchemaKeeperClient {
	return &MockSchemaKeeperClient{
		Configuration: cfg,
		SubjectSchemas: &syncmap.Map{},
		IdToSchema:     &syncmap.Map{},
	}
}
