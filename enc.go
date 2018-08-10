package main

import (
	"encoding/json"
	"strings"
)

// Encoder interface is for all register encoders
type Encoder interface {
	Encode(v interface{}) ([]byte, error)
	Decode(data []byte, vPtr interface{}) error
}

//TODO implement encode/decode from msgp

//JsonEncoder is a JSON Encoder implementation
// This encoder will use the builtin encoding/json to Marshal
// and Unmarshal most types, including structs.
type JsonEncoder struct {
	// Empty
}

//Encode
func (je *JsonEncoder) Encode(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

//Decode
func (je *JsonEncoder) Decode(data []byte, vPtr interface{}) (err error) {
	switch arg := vPtr.(type) {
	case *string:
		// If they want a string and it is a JSON string, strip quotes
		// This allows someone to send a struct but receive as a plain string
		// This cast should be efficient for Go 1.3 and beyond.
		str := string(data)
		if strings.HasPrefix(str, `"`) && strings.HasSuffix(str, `"`) {
			*arg = str[1 : len(str)-1]
		} else {
			*arg = str
		}
	case *[]byte:
		*arg = data
	default:
		err = json.Unmarshal(data, arg)
	}
	return
}
