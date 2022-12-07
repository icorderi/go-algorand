package encoded

// Code generated by github.com/algorand/msgp DO NOT EDIT.

import (
	"sort"

	"github.com/algorand/msgp/msgp"
)

// The following msgp objects are implemented in this file:
// BalanceRecordV6
//        |-----> (*) MarshalMsg
//        |-----> (*) CanMarshalMsg
//        |-----> (*) UnmarshalMsg
//        |-----> (*) CanUnmarshalMsg
//        |-----> (*) Msgsize
//        |-----> (*) MsgIsZero
//
// KVRecordV6
//      |-----> (*) MarshalMsg
//      |-----> (*) CanMarshalMsg
//      |-----> (*) UnmarshalMsg
//      |-----> (*) CanUnmarshalMsg
//      |-----> (*) Msgsize
//      |-----> (*) MsgIsZero
//

// MarshalMsg implements msgp.Marshaler
func (z *BalanceRecordV6) MarshalMsg(b []byte) (o []byte) {
	o = msgp.Require(b, z.Msgsize())
	// omitempty: check for empty values
	zb0003Len := uint32(4)
	var zb0003Mask uint8 /* 5 bits */
	if (*z).Address.MsgIsZero() {
		zb0003Len--
		zb0003Mask |= 0x2
	}
	if (*z).AccountData.MsgIsZero() {
		zb0003Len--
		zb0003Mask |= 0x4
	}
	if len((*z).Resources) == 0 {
		zb0003Len--
		zb0003Mask |= 0x8
	}
	if (*z).ExpectingMoreEntries == false {
		zb0003Len--
		zb0003Mask |= 0x10
	}
	// variable map header, size zb0003Len
	o = append(o, 0x80|uint8(zb0003Len))
	if zb0003Len != 0 {
		if (zb0003Mask & 0x2) == 0 { // if not empty
			// string "a"
			o = append(o, 0xa1, 0x61)
			o = (*z).Address.MarshalMsg(o)
		}
		if (zb0003Mask & 0x4) == 0 { // if not empty
			// string "b"
			o = append(o, 0xa1, 0x62)
			o = (*z).AccountData.MarshalMsg(o)
		}
		if (zb0003Mask & 0x8) == 0 { // if not empty
			// string "c"
			o = append(o, 0xa1, 0x63)
			if (*z).Resources == nil {
				o = msgp.AppendNil(o)
			} else {
				o = msgp.AppendMapHeader(o, uint32(len((*z).Resources)))
			}
			zb0001_keys := make([]uint64, 0, len((*z).Resources))
			for zb0001 := range (*z).Resources {
				zb0001_keys = append(zb0001_keys, zb0001)
			}
			sort.Sort(SortUint64(zb0001_keys))
			for _, zb0001 := range zb0001_keys {
				zb0002 := (*z).Resources[zb0001]
				_ = zb0002
				o = msgp.AppendUint64(o, zb0001)
				o = zb0002.MarshalMsg(o)
			}
		}
		if (zb0003Mask & 0x10) == 0 { // if not empty
			// string "e"
			o = append(o, 0xa1, 0x65)
			o = msgp.AppendBool(o, (*z).ExpectingMoreEntries)
		}
	}
	return
}

func (_ *BalanceRecordV6) CanMarshalMsg(z interface{}) bool {
	_, ok := (z).(*BalanceRecordV6)
	return ok
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *BalanceRecordV6) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0003 int
	var zb0004 bool
	zb0003, zb0004, bts, err = msgp.ReadMapHeaderBytes(bts)
	if _, ok := err.(msgp.TypeError); ok {
		zb0003, zb0004, bts, err = msgp.ReadArrayHeaderBytes(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		if zb0003 > 0 {
			zb0003--
			bts, err = (*z).Address.UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "struct-from-array", "Address")
				return
			}
		}
		if zb0003 > 0 {
			zb0003--
			bts, err = (*z).AccountData.UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "struct-from-array", "AccountData")
				return
			}
		}
		if zb0003 > 0 {
			zb0003--
			var zb0005 int
			var zb0006 bool
			zb0005, zb0006, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "struct-from-array", "Resources")
				return
			}
			if zb0005 > resourcesPerCatchpointFileChunkBackwardCompatible {
				err = msgp.ErrOverflow(uint64(zb0005), uint64(resourcesPerCatchpointFileChunkBackwardCompatible))
				err = msgp.WrapError(err, "struct-from-array", "Resources")
				return
			}
			if zb0006 {
				(*z).Resources = nil
			} else if (*z).Resources == nil {
				(*z).Resources = make(map[uint64]msgp.Raw, zb0005)
			}
			for zb0005 > 0 {
				var zb0001 uint64
				var zb0002 msgp.Raw
				zb0005--
				zb0001, bts, err = msgp.ReadUint64Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "struct-from-array", "Resources")
					return
				}
				bts, err = zb0002.UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "struct-from-array", "Resources", zb0001)
					return
				}
				(*z).Resources[zb0001] = zb0002
			}
		}
		if zb0003 > 0 {
			zb0003--
			(*z).ExpectingMoreEntries, bts, err = msgp.ReadBoolBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "struct-from-array", "ExpectingMoreEntries")
				return
			}
		}
		if zb0003 > 0 {
			err = msgp.ErrTooManyArrayFields(zb0003)
			if err != nil {
				err = msgp.WrapError(err, "struct-from-array")
				return
			}
		}
	} else {
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		if zb0004 {
			(*z) = BalanceRecordV6{}
		}
		for zb0003 > 0 {
			zb0003--
			field, bts, err = msgp.ReadMapKeyZC(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
			switch string(field) {
			case "a":
				bts, err = (*z).Address.UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "Address")
					return
				}
			case "b":
				bts, err = (*z).AccountData.UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "AccountData")
					return
				}
			case "c":
				var zb0007 int
				var zb0008 bool
				zb0007, zb0008, bts, err = msgp.ReadMapHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Resources")
					return
				}
				if zb0007 > resourcesPerCatchpointFileChunkBackwardCompatible {
					err = msgp.ErrOverflow(uint64(zb0007), uint64(resourcesPerCatchpointFileChunkBackwardCompatible))
					err = msgp.WrapError(err, "Resources")
					return
				}
				if zb0008 {
					(*z).Resources = nil
				} else if (*z).Resources == nil {
					(*z).Resources = make(map[uint64]msgp.Raw, zb0007)
				}
				for zb0007 > 0 {
					var zb0001 uint64
					var zb0002 msgp.Raw
					zb0007--
					zb0001, bts, err = msgp.ReadUint64Bytes(bts)
					if err != nil {
						err = msgp.WrapError(err, "Resources")
						return
					}
					bts, err = zb0002.UnmarshalMsg(bts)
					if err != nil {
						err = msgp.WrapError(err, "Resources", zb0001)
						return
					}
					(*z).Resources[zb0001] = zb0002
				}
			case "e":
				(*z).ExpectingMoreEntries, bts, err = msgp.ReadBoolBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "ExpectingMoreEntries")
					return
				}
			default:
				err = msgp.ErrNoField(string(field))
				if err != nil {
					err = msgp.WrapError(err)
					return
				}
			}
		}
	}
	o = bts
	return
}

func (_ *BalanceRecordV6) CanUnmarshalMsg(z interface{}) bool {
	_, ok := (z).(*BalanceRecordV6)
	return ok
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *BalanceRecordV6) Msgsize() (s int) {
	s = 1 + 2 + (*z).Address.Msgsize() + 2 + (*z).AccountData.Msgsize() + 2 + msgp.MapHeaderSize
	if (*z).Resources != nil {
		for zb0001, zb0002 := range (*z).Resources {
			_ = zb0001
			_ = zb0002
			s += 0 + msgp.Uint64Size + zb0002.Msgsize()
		}
	}
	s += 2 + msgp.BoolSize
	return
}

// MsgIsZero returns whether this is a zero value
func (z *BalanceRecordV6) MsgIsZero() bool {
	return ((*z).Address.MsgIsZero()) && ((*z).AccountData.MsgIsZero()) && (len((*z).Resources) == 0) && ((*z).ExpectingMoreEntries == false)
}

// MarshalMsg implements msgp.Marshaler
func (z *KVRecordV6) MarshalMsg(b []byte) (o []byte) {
	o = msgp.Require(b, z.Msgsize())
	// omitempty: check for empty values
	zb0001Len := uint32(2)
	var zb0001Mask uint8 /* 3 bits */
	if len((*z).Key) == 0 {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	if len((*z).Value) == 0 {
		zb0001Len--
		zb0001Mask |= 0x4
	}
	// variable map header, size zb0001Len
	o = append(o, 0x80|uint8(zb0001Len))
	if zb0001Len != 0 {
		if (zb0001Mask & 0x2) == 0 { // if not empty
			// string "k"
			o = append(o, 0xa1, 0x6b)
			o = msgp.AppendBytes(o, (*z).Key)
		}
		if (zb0001Mask & 0x4) == 0 { // if not empty
			// string "v"
			o = append(o, 0xa1, 0x76)
			o = msgp.AppendBytes(o, (*z).Value)
		}
	}
	return
}

func (_ *KVRecordV6) CanMarshalMsg(z interface{}) bool {
	_, ok := (z).(*KVRecordV6)
	return ok
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *KVRecordV6) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 int
	var zb0002 bool
	zb0001, zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
	if _, ok := err.(msgp.TypeError); ok {
		zb0001, zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		if zb0001 > 0 {
			zb0001--
			var zb0003 int
			zb0003, err = msgp.ReadBytesBytesHeader(bts)
			if err != nil {
				err = msgp.WrapError(err, "struct-from-array", "Key")
				return
			}
			if zb0003 > KVRecordV6MaxKeyLength {
				err = msgp.ErrOverflow(uint64(zb0003), uint64(KVRecordV6MaxKeyLength))
				return
			}
			(*z).Key, bts, err = msgp.ReadBytesBytes(bts, (*z).Key)
			if err != nil {
				err = msgp.WrapError(err, "struct-from-array", "Key")
				return
			}
		}
		if zb0001 > 0 {
			zb0001--
			var zb0004 int
			zb0004, err = msgp.ReadBytesBytesHeader(bts)
			if err != nil {
				err = msgp.WrapError(err, "struct-from-array", "Value")
				return
			}
			if zb0004 > KVRecordV6MaxValueLength {
				err = msgp.ErrOverflow(uint64(zb0004), uint64(KVRecordV6MaxValueLength))
				return
			}
			(*z).Value, bts, err = msgp.ReadBytesBytes(bts, (*z).Value)
			if err != nil {
				err = msgp.WrapError(err, "struct-from-array", "Value")
				return
			}
		}
		if zb0001 > 0 {
			err = msgp.ErrTooManyArrayFields(zb0001)
			if err != nil {
				err = msgp.WrapError(err, "struct-from-array")
				return
			}
		}
	} else {
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		if zb0002 {
			(*z) = KVRecordV6{}
		}
		for zb0001 > 0 {
			zb0001--
			field, bts, err = msgp.ReadMapKeyZC(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
			switch string(field) {
			case "k":
				var zb0005 int
				zb0005, err = msgp.ReadBytesBytesHeader(bts)
				if err != nil {
					err = msgp.WrapError(err, "Key")
					return
				}
				if zb0005 > KVRecordV6MaxKeyLength {
					err = msgp.ErrOverflow(uint64(zb0005), uint64(KVRecordV6MaxKeyLength))
					return
				}
				(*z).Key, bts, err = msgp.ReadBytesBytes(bts, (*z).Key)
				if err != nil {
					err = msgp.WrapError(err, "Key")
					return
				}
			case "v":
				var zb0006 int
				zb0006, err = msgp.ReadBytesBytesHeader(bts)
				if err != nil {
					err = msgp.WrapError(err, "Value")
					return
				}
				if zb0006 > KVRecordV6MaxValueLength {
					err = msgp.ErrOverflow(uint64(zb0006), uint64(KVRecordV6MaxValueLength))
					return
				}
				(*z).Value, bts, err = msgp.ReadBytesBytes(bts, (*z).Value)
				if err != nil {
					err = msgp.WrapError(err, "Value")
					return
				}
			default:
				err = msgp.ErrNoField(string(field))
				if err != nil {
					err = msgp.WrapError(err)
					return
				}
			}
		}
	}
	o = bts
	return
}

func (_ *KVRecordV6) CanUnmarshalMsg(z interface{}) bool {
	_, ok := (z).(*KVRecordV6)
	return ok
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *KVRecordV6) Msgsize() (s int) {
	s = 1 + 2 + msgp.BytesPrefixSize + len((*z).Key) + 2 + msgp.BytesPrefixSize + len((*z).Value)
	return
}

// MsgIsZero returns whether this is a zero value
func (z *KVRecordV6) MsgIsZero() bool {
	return (len((*z).Key) == 0) && (len((*z).Value) == 0)
}
