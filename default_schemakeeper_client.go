package schemakeepergo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/linkedin/goavro/v2"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	APiVersion string = "v2"
)

type defaultSchemaKeeperClient struct {
	Configuration Configuration
	Client        *http.Client
}

func (d *defaultSchemaKeeperClient) GetSchemaById(id int32) (*goavro.Codec, error) {
	url := fmt.Sprintf("%s/%s/schemas/%d", d.Configuration.GetSchemaKeeperUrl(), APiVersion, id)
	resp, err := d.Client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(bodyBytes))
	}

	metadata := &SchemaMetadata{}

	err = json.NewDecoder(resp.Body).Decode(metadata)
	if err != nil {
		return nil, err
	}

	return goavro.NewCodec(metadata.SchemaText)
}

func (d *defaultSchemaKeeperClient) RegisterNewSchema(subject string, schema *goavro.Codec, schemaType SchemaType, compatibilityType CompatibilityType) (int32, error) {
	url := fmt.Sprintf("%s/%s/subjects/%s/schemas", d.Configuration.GetSchemaKeeperUrl(), APiVersion, subject)
	body := &NewSubjectAndSchema{schema.Schema(), string(schemaType), string(compatibilityType)}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return -1, err
	}

	resp, err := d.Client.Post(url, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return -1, err
		}
		return -1, errors.New(string(bodyBytes))
	}

	metadata := &SchemaId{}

	err = json.NewDecoder(resp.Body).Decode(metadata)
	if err != nil {
		return -1, err
	}
	return metadata.SchemaId, nil
}

func (d *defaultSchemaKeeperClient) GetSchemaId(subject string, schema *goavro.Codec, schemaType SchemaType) (int32, error) {
	url := fmt.Sprintf("%s/%s/subjects/%s/schemas/id", d.Configuration.GetSchemaKeeperUrl(), APiVersion, subject)
	body := &SchemaText{schema.Schema(), string(schemaType)}
	jsonBody, err := json.Marshal(body)

	resp, err := d.Client.Post(url, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return -1, err
		}
		return -1, errors.New(string(bodyBytes))
	}

	metadata := &SchemaId{}

	err = json.NewDecoder(resp.Body).Decode(metadata)
	if err != nil {
		return -1, err
	}
	return metadata.SchemaId, nil
}

func (d *defaultSchemaKeeperClient) Close() error {
	d.Client.CloseIdleConnections()
	return nil
}

func CreateDefaultSchemaKeeperClient(cfg Configuration) SchemaKeeperClient {
	client := &http.Client{Timeout: 5 * time.Second}
	return &defaultSchemaKeeperClient{
		Configuration: cfg,
		Client:        client,
	}
}
