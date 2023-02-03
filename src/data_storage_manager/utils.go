package data_storage_manager

import "encoding/binary"

func bytes2Int32(bytes []byte) int32 {
	return int32(binary.BigEndian.Uint32(bytes))
}

func int32ToBytes(i int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf
}
