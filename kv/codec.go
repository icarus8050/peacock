package kv

import (
	"encoding/binary"
	"errors"
)

var ErrCorruptPayload = errors.New("kv: corrupt payload")

const keyLenSize = 4

// encodePayload serializes (key, value) into Entry.Data.
//
// Layout (little-endian): KeyLen(4) | Key | Value
func encodePayload(key string, value []byte) []byte {
	buf := make([]byte, keyLenSize+len(key)+len(value))
	binary.LittleEndian.PutUint32(buf[0:keyLenSize], uint32(len(key)))
	copy(buf[keyLenSize:keyLenSize+len(key)], key)
	copy(buf[keyLenSize+len(key):], value)
	return buf
}

func decodePayload(data []byte) (string, []byte, error) {
	if len(data) < keyLenSize {
		return "", nil, ErrCorruptPayload
	}
	keyLen := int(binary.LittleEndian.Uint32(data[0:keyLenSize]))
	if len(data) < keyLenSize+keyLen {
		return "", nil, ErrCorruptPayload
	}
	key := string(data[keyLenSize : keyLenSize+keyLen])
	value := append([]byte(nil), data[keyLenSize+keyLen:]...)
	return key, value, nil
}
