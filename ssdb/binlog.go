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
	body := bytes[0][OFFSET:]
	binlog.body = bytes

	if len(body) > 1 {
		offset := 0
		cmdtag := body[offset]
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
				string(body[offset:]),
				string(bytes[1]),
			}
		case BINLOGCOMMAND_KDEL:
			if cmdtag != DATATYPE_KV {
				break
			}
			binlog.cmd = []string{
				"del",
				string(body[offset:]),
			}
		case BINLOGCOMMAND_HSET:
			if len(bytes) != 2 {
				break
			}
			if cmdtag != DATATYPE_HASH {
				break
			}
			size := int(body[offset])
			offset += 1
			name := string(body[offset : offset+size])
			offset = offset + size
			offset += 1
			key := string(body[offset:])
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
			size := int(body[offset])
			offset += 1
			name := string(body[offset : offset+size])
			offset = offset + size
			offset += 1
			key := string(body[offset:])
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
			size := int(body[offset])
			offset += 1
			name := string(body[offset : offset+size])
			offset = offset + size
			sign := ""
			if body[offset] == '-' {
				sign = "-"
			}
			offset += 1
			key := string(body[offset:])
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
			size := int(body[offset])
			offset += 1
			name := string(body[offset : offset+size])
			offset = offset + size
			offset += 1
			key := string(body[offset:])
			if name != SSDB_EXPIRATION_LIST_KEY {
				binlog.cmd = []string{
					"zrem",
					name,
					key,
				}
			}
		case BINLOGCOMMAND_QSET, BINLOGCOMMAND_QPUSH_BACK, BINLOGCOMMAND_QPUSH_FRONT:
			if len(bytes) != 2 {
				break
			}
			size := int(body[offset])
			offset += 1
			name := string(body[offset : offset+size])
			offset = offset + size
			seq := binary.BigEndian.Uint64(body[offset:])
			if seq < SSDB_QITEM_MIN_SEQ || seq > SSDB_QITEM_MAX_SEQ {
				break
			}
			val := string(bytes[1]) //list val
			switch binlog.cmdtype{
			case BINLOGCOMMAND_QPUSH_BACK:
				binlog.cmd = []string{
					"rpush",
					name,
					val,
				}
			case BINLOGCOMMAND_QPUSH_FRONT:
				binlog.cmd = []string{
					"lpush",
					name,
					val,
				}
			case BINLOGCOMMAND_QSET:
				fmt.Println("unsuported qset binlog:", name, seq, val)
			default:
				fmt.Println("unknown qet/push binlog:", binlog)
			}
		case BINLOGCOMMAND_QPOP_BACK,BINLOGCOMMAND_QPOP_FRONT:
			name := string(body[:])
			switch binlog.cmdtype{
			case BINLOGCOMMAND_QPOP_BACK:
				binlog.cmd = []string{
					"rpop",
					name,
				}
			case BINLOGCOMMAND_QPOP_FRONT:
				binlog.cmd = []string{
					"lpop",
					name,
				}
			default:
				fmt.Println("unknown pop binlog:", binlog)
			}
		default:
			fmt.Println("unknown binlog:", binlog)
		}

	}
	return
}
