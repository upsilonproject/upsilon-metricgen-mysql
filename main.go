package main

import (
	log "github.com/sirupsen/logrus"
	"time"
	"database/sql"
	metricgen "github.com/upsilonproject/upsilon-metricgen-mysql/internal"
	updb "github.com/upsilonproject/upsilon-golib-database/pkg/database"
)

var (
	dbUpsilon *sql.DB;
)

func main() {
	log.Infof("upsilon-metricgen-mysql \033];upsilon-metricgen-mysql\a")

	dbUpsilon = metricgen.DbConn("upsilon")

	stmtInsert := updb.PrepareMetricInsert(dbUpsilon)

	for true {
		metricgen.RunServiceLoop(dbUpsilon, stmtInsert);
		log.Infof("--- Chunk complete, sleeping")
		time.Sleep(10 * time.Second)
	}
}
