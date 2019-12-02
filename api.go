package schemakeepergo

type SchemaMetadata struct {
	SchemaId   int32  `json:"schemaId" binding:"required"`
	SchemaText string `json:"schemaText" binding:"required"`
	SchemaHash string `json:"schemaHash" binding:"required"`
	SchemaType string `json:"schemaType" binding:"required"`
}

type SchemaId struct {
	SchemaId int32 `json:"schemaId" binding:"required"`
}

type NewSubjectAndSchema struct {
	SchemaText        string `json:"schemaText" binding:"required"`
	SchemaType        string `json:"schemaType" binding:"required"`
	CompatibilityType string `json:"compatibilityType" binding:"required"`
}

type SchemaText struct {
	SchemaText string `json:"schemaText" binding:"required"`
	SchemaType string `json:"schemaType" binding:"required"`
}
