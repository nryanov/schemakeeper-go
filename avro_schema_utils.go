package schemakeepergo

import "errors"

const (
	AvroByte byte = 0b1111001
	AvroCompatibleMask byte = 0b1111
)

func CheckByte(b byte) error {
	if ((b >> 3) ^ AvroCompatibleMask) != 0 {
		return errors.New("schema type byte is not avro compatible")
	}

	return nil
}