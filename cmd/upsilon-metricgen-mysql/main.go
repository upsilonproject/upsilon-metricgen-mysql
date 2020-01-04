package main

import (
	"log"
	"time"
	"database/sql"
	metricgen "github.com/upsilonproject/upsilon-metricgen-mysql/pkg/database"
)

var (
	dbUpsilon *sql.DB;
	dbMetrics *sql.DB;
)

func main() {
	log.SetPrefix("metricgen ")

	dbUpsilon = metricgen.DbConn("upsilon")
	dbMetrics = metricgen.DbConn("upsilon_results")

	stmtInsert := metricgen.PrepareMetricInsert(dbMetrics)

	for true {
		metricgen.RunServiceLoop(dbUpsilon, dbMetrics, stmtInsert);
		log.Println("Chunk complete, sleeping")
		time.Sleep(30 * time.Second)
	}
}
