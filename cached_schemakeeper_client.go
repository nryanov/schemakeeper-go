package schemakeepergo

import (
	"github.com/linkedin/goavro/v2"
	"golang.org/x/sync/syncmap"
)

type cachedSchemaKeeperClient struct {
	Client         SchemaKeeperClient
	SubjectSchemas *syncmap.Map
	IdToSchema     *syncmap.Map
}

func (c *cachedSchemaKeeperClient) GetSchemaById(id int32) (*goavro.Codec, error) {
	schema, exist := c.IdToSchema.Load(id)
	if !exist {
		newSchema, err := c.Client.GetSchemaById(id)
		if err != nil {
			return nil, err
		} else {
			c.IdToSchema.Store(id, newSchema)
			return newSchema, nil
		}
	} else {
		return schema.(*goavro.Codec), nil
	}
}

func (c *cachedSchemaKeeperClient) RegisterNewSchema(subject string, schema *goavro.Codec, schemaType SchemaType, compatibilityType CompatibilityType) (int32, error) {
	schemas, _ := c.SubjectSchemas.LoadOrStore(subject, &syncmap.Map{})

	id, exist := schemas.(*syncmap.Map).Load(schema.Schema())
	if !exist {
		id, err := c.Client.RegisterNewSchema(subject, schema, schemaType, compatibilityType)
		if err != nil {
			return -1, err
		} else {
			schemas.(*syncmap.Map).Store(schema.Schema(), id)
			return id, nil
		}
	} else {
		return id.(int32), nil
	}
}

func (c *cachedSchemaKeeperClient) GetSchemaId(subject string, schema *goavro.Codec, schemaType SchemaType) (int32, error) {
	schemas, _ := c.SubjectSchemas.LoadOrStore(subject, &syncmap.Map{})

	id, exist := schemas.(*syncmap.Map).Load(schema.Schema())
	if !exist {
		id, err := c.Client.GetSchemaId(subject, schema, schemaType)
		if err != nil {
			return -1, err
		} else {
			schemas.(*syncmap.Map).Store(schema.Schema(), id)
			return id, nil
		}
	} else {
		return id.(int32), nil
	}
}

func (c *cachedSchemaKeeperClient) Close() error {
	return c.Client.Close()
}

func CreateCachedSchemaKeeperClient(cfg Configuration) SchemaKeeperClient {
	defaultClient := CreateDefaultSchemaKeeperClient(cfg)
	return &cachedSchemaKeeperClient{
		Client:         defaultClient,
		SubjectSchemas: &syncmap.Map{},
		IdToSchema:     &syncmap.Map{},
	}
}

func CreateCachedSchemaKeeperClientUsingCustomClient(client SchemaKeeperClient) SchemaKeeperClient {
	return &cachedSchemaKeeperClient{
		Client:         client,
		SubjectSchemas: &syncmap.Map{},
		IdToSchema:     &syncmap.Map{},
	}
}
