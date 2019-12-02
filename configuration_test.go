package schemakeepergo

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReturnDefaultCompatibilityType(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	compatibilityType := cfg.GetDefaultCompatibilityType()
	if compatibilityType != Backward {
		t.Errorf("Default compatibility type is not Backward")
	}
}

func TestReturnSpecifiedCompatibilityType(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	cfg.SetDefaultCompatibilityType(Full)
	compatibilityType := cfg.GetDefaultCompatibilityType()
	if compatibilityType != Full {
		t.Errorf("Default compatibility type is not Full")
	}
}

func TestForceSchemaCreationFlagDefaultTrue(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	flag := cfg.IsForceSchemaRegistrationAllowed()
	if !flag {
		t.Errorf("Force schema creation should be true by default")
	}
}

func TestForceSchemaCreationFlagTrue(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	cfg.SetForceSchemaRegistration(true)
	flag := cfg.IsForceSchemaRegistrationAllowed()
	if !flag {
		t.Errorf("Force schema creation should be true")
	}
}

func TestForceSchemaCreationFlagFalse(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	cfg.SetForceSchemaRegistration(false)
	flag := cfg.IsForceSchemaRegistrationAllowed()
	if flag {
		t.Errorf("Force schema creation should be false")
	}
}

func TestUrl(t *testing.T) {
	cfg := CreateConfiguration("host:port")
	url := cfg.GetSchemaKeeperUrl()
	if url != "host:port" {
		t.Errorf("Schemakeeper URL is not equal to host:port")
	}
}

func TestSetEmptyUrl(t *testing.T) {
	cfg := CreateConfiguration("host:port")

	assert.Panics(t, func() { cfg.SetSchemaKeeperUrl("") }, "The code did not panic")
}

func TestCreateConfigWithEmptyUrl(t *testing.T) {
	assert.Panics(t, func() { CreateConfiguration("") }, "The code did not panic")
}