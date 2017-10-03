package ssdb

import (
	"encoding/binary"
	"fmt"
	//	"strconv"
)

var (
	SEQ_SIZE      = 8
	DATATYPE_SIZE = 1
	CMD_SIZE      = 1
)

type Binlog struct {
	seq      uint64
	datatype uint8
	cmdtype  uint8
	cmd      []string
	body     [][]byte
}

func LoadBinlog(bytes [][]byte) (binlog *Binlog, err error) {
	binlog = &Binlog{}
	OFFSET := 0
	binlog.seq = binary.LittleEndian.Uint64(bytes[0][OFFSET:SEQ_SIZE])
	OFFSET += SEQ_SIZE
	binlog.datatype = (uint8)(bytes[0][OFFSET])
	OFFSET += DATATYPE_SIZE
	binlog.cmdtype = (uint8)(bytes[0][OFFSET])
	OFFSET += CMD_SIZE
	key := bytes[0][OFFSET:]
	binlog.body = bytes

	if len(key) > 1 {
		offset := 0
		cmdtag := key[offset]
		offset += 1 //first byte is cmd tag,second byte is split byte
		switch binlog.cmdtype {
		case BINLOGCOMMAND_KSET:
			if len(bytes) != 2 {
				break
			}
			if cmdtag != DATATYPE_KV {
				break
			}
			binlog.cmd = []string{
				"set",
				string(key[offset:]),
				string(bytes[1]),
			}
		case BINLOGCOMMAND_KDEL:
			if cmdtag != DATATYPE_KV {
				break
			}
			binlog.cmd = []string{
				"del",
				string(key[offset]),
			}
		case BINLOGCOMMAND_HSET:
			if len(bytes) != 2 {
				break
			}
			if cmdtag != DATATYPE_HASH {
				break
			}
			size := int(key[offset])
			offset += 1
			name := string(key[offset : offset+size])
			offset = offset + size
			offset += 1
			key := string(key[offset:])
			val := string(bytes[1])
			binlog.cmd = []string{
				"hset",
				name,
				key,
				val,
			}
		case BINLOGCOMMAND_HDEL:
			if cmdtag != DATATYPE_HASH {
				break
			}
			size := int(key[offset])
			offset += 1
			name := string(key[offset : offset+size])
			offset = offset + size
			offset += 1
			key := string(key[offset:])
			binlog.cmd = []string{
				"hdel",
				name,
				key,
			}
		case BINLOGCOMMAND_ZSET:
			if len(bytes) != 2 {
				break
			}
			if cmdtag != DATATYPE_ZSET {
				break
			}
			size := int(key[offset])
			offset += 1
			name := string(key[offset : offset+size])
			offset = offset + size
			sign := ""
			if key[offset] == '-' {
				sign = "-"
			}
			offset += 1
			key := string(key[offset:])
			val := sign + string(bytes[1]) //add number's sign
			if name != SSDB_EXPIRATION_LIST_KEY {
				binlog.cmd = []string{
					"zadd",
					name,
					val,
					key,
				}
			} else {
				// set ttl key
				binlog.cmd = []string{
					"expireat",
					key,
					val[0:10],
				}
			}
		case BINLOGCOMMAND_ZDEL:
			if cmdtag != DATATYPE_ZSET {
				break
			}
			size := int(key[offset])
			offset += 1
			name := string(key[offset : offset+size])
			offset = offset + size
			offset += 1
			key := string(key[offset:])
			if name != SSDB_EXPIRATION_LIST_KEY {
				binlog.cmd = []string{
					"zrem",
					name,
					key,
				}
			}
		default:
			fmt.Println("unknown binlog:", binlog)
		}

	}
	return
}
