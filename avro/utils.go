package avro

import (
	"errors"
	"fmt"
)

// ErrNilItem indicates that the provided item was nil, but must be non-nil
var ErrNilItem = errors.New("item was nil")

// ErrBranchNode indicates that the proved item was an internal node to the data tree, but must be a leaf node
var ErrBranchNode = errors.New("item was not an avro leaf")

// ErrEmptyItem indicates that the provided map was non-nil, but did not contain the expected single key + value
var ErrEmptyItem = errors.New("item map was empty")

// ExtractAvroString reads an Avro string object
func ExtractAvroString(item interface{}) (string, error) {
	i, err := extractAvroInterface(item, "string")
	if err != nil {
		return "", err
	}
	return i.(string), nil
}

// ExtractAvroLong reads an Avro long object
func ExtractAvroLong(item interface{}) (int64, error) {
	i, err := extractAvroInterface(item, "long")
	if err != nil {
		return 0, err
	}
	return i.(int64), nil
}

// ExtractAvroBool reads an Avro boolean object
func ExtractAvroBool(item interface{}) (bool, error) {
	i, err := extractAvroInterface(item, "boolean")
	if err != nil {
		return false, err
	}
	return i.(bool), nil
}

// ExtractAvroFloat32 reads an Avro float object
func ExtractAvroFloat32(item interface{}) (float32, error) {
	i, err := extractAvroInterface(item, "float")
	if err != nil {
		return 0, err
	}
	return i.(float32), nil
}

// ExtractAvroFloat64 reads an Avro double object
func ExtractAvroFloat64(item interface{}) (float64, error) {
	i, err := extractAvroInterface(item, "double")
	if err != nil {
		return 0, err
	}
	return i.(float64), nil
}

// ExtractAvroBytes reads an Avro bytes object
func ExtractAvroBytes(item interface{}) ([]byte, error) {
	i, err := extractAvroInterface(item, "bytes")
	if err != nil {
		return nil, err
	}
	return i.([]byte), nil
}

func extractAvroInterface(item interface{}, typeName string) (interface{}, error) {
	if item == nil {
		return nil, ErrNilItem
	}
	mapItem, ok := item.(map[string]interface{})
	if !ok {
		return "", ErrBranchNode
	}
	for k, v := range mapItem {
		if k == typeName {
			return v, nil
		}
		return nil, fmt.Errorf("item was `%s`, not `%s`", k, typeName)
	}
	return nil, ErrEmptyItem
}
