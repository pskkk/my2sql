package base

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	toolkits "my2sql/toolkits"
)

var (
	fileBinEventHandlingIndex uint64 = 0
	fileTrxIndex              uint64 = 0
)


type BinFileParser struct {
	Parser *replication.BinlogParser
}


func (this BinFileParser) MyParseAllBinlogFiles(cfg *ConfCmd) {
	defer cfg.CloseChan()
	log.Info("start to parse binlog from local files")
	binlog, binpos := GetFirstBinlogPosToParse(cfg)
	binBaseName, binBaseIndx := GetBinlogBasenameAndIndex(binlog)
	log.Info(fmt.Sprintf("start to parse %s %d\n", binlog, binpos))

	for {
		if cfg.IfSetStopFilePos {
			// 比较 binlog 先后顺序，若 小于 1 则说明有问题，break
			if cfg.StopFilePos.Compare(mysql.Position{Name: filepath.Base(binlog), Pos: 4}) < 1 {
				break
			}
		}

		log.Info(fmt.Sprintf("start to parse %s %d\n", binlog, binpos))
		result, err := this.MyParseOneBinlogFile(cfg, binlog)
		if err != nil {
			log.Error(fmt.Sprintf("error to parse binlog %s %v", binlog, err))
			break
		}

		if result == C_reBreak {
			break
		} else if result == C_reFileEnd {
			if !cfg.IfSetStopParsPoint && !cfg.IfSetStopDateTime {
				//just parse one binlog
				break
			}
			binlog = filepath.Join(cfg.BinlogDir, GetNextBinlog(binBaseName, binBaseIndx))
			if !toolkits.IsFile(binlog) {
				log.Info(fmt.Sprintf("%s not exists nor a file\n", binlog))
				break
			}
			binBaseIndx++
			binpos = 4
		} else {
			log.Info(fmt.Sprintf("this should not happen: return value of MyParseOneBinlog is %d\n", result))
			break
		}

	}
	log.Info("finish parsing binlog from local files")

}
// MyParseOneBinlogFile 读取文件开头的4字节，以此判断文件是否为合法的 binlog，是合法的binlog 则 返回 this.MyParseReader(cfg, f, &binlog)
func (this BinFileParser) MyParseOneBinlogFile(cfg *ConfCmd, name string) (int, error) {
	// process: 0, continue: 1, break: 2
	f, err := os.Open(name) // 打开 binlog
	if f != nil {
		defer f.Close()
	}
	if err != nil {
		log.Error(fmt.Sprintf("fail to open %s %v\n", name, err))
		return C_reBreak, errors.Trace(err)
	}

	fileTypeBytes := int64(4)

	b := make([]byte, fileTypeBytes) // 4 缓冲 []byte

	if _, err = f.Read(b); err != nil { // 读取开头 4字节 binlogHeader
		log.Error(fmt.Sprintf("fail to read %s %v", name, err))
		return C_reBreak, errors.Trace(err)
	} else if !bytes.Equal(b, replication.BinLogFileHeader) { // 判断读取到的 4字节 内容，是否为合法的 binlog Header
		log.Error(fmt.Sprintf("%s is not a valid binlog file, head 4 bytes must fe'bin' ", name))
		return C_reBreak, errors.Trace(err)
	}

	// must not seek to other position, otherwise the program may panic because formatevent, table map event is skipped
	if _, err = f.Seek(fileTypeBytes, os.SEEK_SET); err != nil { // 已经读了 4字节，所以 偏移 4字节
		log.Error(fmt.Sprintf("error seek %s to %d", name, fileTypeBytes))
		return C_reBreak, errors.Trace(err)
	}
	var binlog string = filepath.Base(name)
	return this.MyParseReader(cfg, f, &binlog) // 传入 命令行参数struct，偏移4字节的 文件句柄 *os.File ， 不包含路径的binlog名称
}

// MyParseReader
func (this BinFileParser) MyParseReader(cfg *ConfCmd, r io.Reader, binlog *string) (int, error) {
	// process: 0, continue: 1, break: 2, EOF: 3
	var (
		err         error
		n           int64
		db          string = ""
		tb          string = ""
		sql         string = ""
		sqlType     string = ""
		rowCnt      uint32 = 0
		trxStatus   int    = 0
		sqlLower    string = ""
		tbMapPos    uint32 = 0
	)

	for {
		headBuf := make([]byte, replication.EventHeaderSize) // EventHeaderSize = 19 , 长度 19 的 []byte

		if _, err = io.ReadFull(r, headBuf); err == io.EOF { // 每次，从文件 r 读取 19 字节(Event 头信息);如果是 io.EOF 则说明文件读取完毕，跳出循环
			return C_reFileEnd, nil
		} else if err != nil {
			log.Error(fmt.Sprintf("fail to read binlog event header of %s %v", *binlog, err))
			return C_reBreak, errors.Trace(err)
		}


		var h *replication.EventHeader
		// 解析 Event Header 信息，获取到一个 就偏移 一部分，最终获取到完整的 EventHeader
		h, err = this.Parser.ParseHeader(headBuf)
		if err != nil {
			log.Error(fmt.Sprintf("fail to parse binlog event header of %s %v" , *binlog, err))
			return C_reBreak, errors.Trace(err)
		}
		//fmt.Printf("parsing %s %d %s\n", *binlog, h.LogPos, GetDatetimeStr(int64(h.Timestamp), int64(0), DATETIME_FORMAT))

		if h.EventSize <= uint32(replication.EventHeaderSize) {
			err = errors.Errorf("invalid event header, event size is %d, too small", h.EventSize)
			log.Error("%v", err)
			return C_reBreak, err
		}

		var buf bytes.Buffer
		if n, err = io.CopyN(&buf, r, int64(h.EventSize)-int64(replication.EventHeaderSize)); err != nil { // 不读取EventHeader信息，其余的存入 buf 中
			err = errors.Errorf("get event body err %v, need %d - %d, but got %d", err, h.EventSize, replication.EventHeaderSize, n)
			log.Error("%v", err)
			return C_reBreak, err
		}


		//h.Dump(os.Stdout)

		data := buf.Bytes() // buf 转为 []byte
		var rawData []byte
		rawData = append(rawData, headBuf...)
		rawData = append(rawData, data...)

		eventLen := int(h.EventSize) - replication.EventHeaderSize

		if len(data) != eventLen {
			err = errors.Errorf("invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen)
			log.Errorf("%v", err)
			return C_reBreak, err
		}

		var e replication.Event
		e, err = this.Parser.ParseEvent(h, data, rawData)
		if err != nil {
			log.Error(fmt.Sprintf("fail to parse binlog event body of %s %v",*binlog, err))
			return C_reBreak, errors.Trace(err)
		}
		if h.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = h.LogPos - h.EventSize // avoid mysqlbing mask the row event as unknown table row event
		}

		//e.Dump(os.Stdout)
		//can not advance this check, because we need to parse table map event or table may not found. Also we must seek ahead the read file position
		chRe := CheckBinHeaderCondition(cfg, h, *binlog)
		if chRe == C_reBreak {
			return C_reBreak, nil
		} else if chRe == C_reContinue {
			continue
		} else if chRe == C_reFileEnd {
			return C_reFileEnd, nil
		}

		//binEvent := &replication.BinlogEvent{RawData: rawData, Header: h, Event: e}
		binEvent := &replication.BinlogEvent{Header: h, Event: e} // we donnot need raw data
		oneMyEvent := &MyBinEvent{MyPos: mysql.Position{Name: *binlog, Pos: h.LogPos},
			StartPos: tbMapPos}
		//StartPos: h.LogPos - h.EventSize}
		chRe = oneMyEvent.CheckBinEvent(cfg, binEvent, binlog)
		if chRe == C_reBreak {
			return C_reBreak, nil
		} else if chRe == C_reContinue {
			continue
		} else if chRe == C_reFileEnd {
			return C_reFileEnd, nil
		} 

		db, tb, sqlType, sql, rowCnt = GetDbTbAndQueryAndRowCntFromBinevent(binEvent)
		if sqlType == "query" {
			sqlLower = strings.ToLower(sql)
			if sqlLower == "begin" {
				trxStatus = C_trxBegin
				fileTrxIndex++
			} else if sqlLower == "commit" {
				trxStatus = C_trxCommit
			} else if sqlLower == "rollback" {
				trxStatus = C_trxRollback
			} else if oneMyEvent.QuerySql != nil {
				trxStatus = C_trxProcess
				rowCnt = 1
			}
		} else {
			trxStatus = C_trxProcess
		}


		if cfg.WorkType != "stats" {
			ifSendEvent := false
			if oneMyEvent.IfRowsEvent {

				tbKey := GetAbsTableName(string(oneMyEvent.BinEvent.Table.Schema),
						string(oneMyEvent.BinEvent.Table.Table))
				_, err = G_TablesColumnsInfo.GetTableInfoJson(string(oneMyEvent.BinEvent.Table.Schema),
						string(oneMyEvent.BinEvent.Table.Table))
				if err != nil {
					log.Fatalf(fmt.Sprintf("no table struct found for %s, it maybe dropped, skip it. RowsEvent position:%s",
							tbKey, oneMyEvent.MyPos.String()))
				}
				ifSendEvent = true
			}

			if ifSendEvent {
				fileBinEventHandlingIndex++
				oneMyEvent.EventIdx = fileBinEventHandlingIndex
				oneMyEvent.SqlType = sqlType
				oneMyEvent.Timestamp = h.Timestamp
				oneMyEvent.TrxIndex = fileTrxIndex
				oneMyEvent.TrxStatus = trxStatus
				cfg.EventChan <- *oneMyEvent
			}


		} 

		//output analysis result whatever the WorkType is	
		if sqlType != "" {
			if sqlType == "query" {
				cfg.StatChan <- BinEventStats{Timestamp: h.Timestamp, Binlog: *binlog, StartPos: h.LogPos - h.EventSize, StopPos: h.LogPos,
					Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
			} else {
				cfg.StatChan <- BinEventStats{Timestamp: h.Timestamp, Binlog: *binlog, StartPos: tbMapPos, StopPos: h.LogPos,
					Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
			}
		}


	}

	return C_reFileEnd, nil
}

