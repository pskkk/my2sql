package main

import (
	"sync"

	"github.com/siddontang/go-mysql/replication"
	my "my2sql/base"
)
// ./main  -user root -password root -host 192.168.109.129   -port 3306  -mode file -local-binlog-file ./mysqlbin.000011 -work-type rollback  -start-file mysqlbin.000011 -output-dir ./tmpdir
func main() {
	my.GConfCmd.IfSetStopParsPoint = false
	my.GConfCmd.ParseCmdOptions()
	defer my.GConfCmd.CloseFH()
	if my.GConfCmd.WorkType != "stats" {
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1, Finished: false}
	}
	var wg, wgGenSql sync.WaitGroup
	wg.Add(1)
	go my.ProcessBinEventStats(my.GConfCmd, &wg)

	if my.GConfCmd.WorkType != "stats" {
		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(my.GConfCmd, &wg)
		for i := uint(1); i <= my.GConfCmd.Threads; i++ {
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, my.GConfCmd, &wgGenSql)
		}
	}
	if my.GConfCmd.Mode == "repl" {
		my.ParserAllBinEventsFromRepl(my.GConfCmd)
	} else if my.GConfCmd.Mode == "file" {
		myParser := my.BinFileParser{}
		myParser.Parser = replication.NewBinlogParser()
		// donot parse mysql datetime/time column into go time structure, take it as string
		myParser.Parser.SetParseTime(false) 
		// sqlbuilder not support decimal type 
		myParser.Parser.SetUseDecimal(false) 
		myParser.MyParseAllBinlogFiles(my.GConfCmd) // 开始解析
	}
	wgGenSql.Wait()
	close(my.GConfCmd.SqlChan)
	wg.Wait() 
}



