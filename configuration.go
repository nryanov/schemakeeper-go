package schemakeepergo

import (
	"errors"
)

type Configuration interface {
	GetSchemaKeeperUrl() string

	IsForceSchemaRegistrationAllowed() bool

	GetDefaultCompatibilityType() CompatibilityType

	SetSchemaKeeperUrl(host string)

	SetForceSchemaRegistration(flag bool)

	SetDefaultCompatibilityType(compatibilityType CompatibilityType)
}

type configuration struct {
	SchemaKeeperUrl          string
	AllowForceSchemaRegister bool
	DefaultCompatibilityType CompatibilityType
}

func (c *configuration) GetSchemaKeeperUrl() string {
	return c.SchemaKeeperUrl
}

func (c *configuration) IsForceSchemaRegistrationAllowed() bool {
	return c.AllowForceSchemaRegister
}

func (c *configuration) GetDefaultCompatibilityType() CompatibilityType {
	return c.DefaultCompatibilityType
}

func (c *configuration) SetSchemaKeeperUrl(schemaKeeperUrl string) {
	if len(schemaKeeperUrl) == 0 {
		panic(errors.New("SchemaKeeperUrl should not be empty"))
	}

	c.SchemaKeeperUrl = schemaKeeperUrl
}

func (c *configuration) SetForceSchemaRegistration(flag bool) {
	c.AllowForceSchemaRegister = flag
}

func (c *configuration) SetDefaultCompatibilityType(compatibilityType CompatibilityType) {
	c.DefaultCompatibilityType = compatibilityType
}

func CreateConfiguration(schemaKeeperUrl string) Configuration {
	if len(schemaKeeperUrl) == 0 {
		panic(errors.New("SchemaKeeperUrl should not be empty"))
	}

	return &configuration{schemaKeeperUrl, true, Backward}
}
