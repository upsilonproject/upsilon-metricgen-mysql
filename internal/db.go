package metricgen;

import (
	"strings"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"


	. "github.com/upsilonproject/upsilon-golib-database/pkg/models"
)

func DbConn(dbName string) (db *sql.DB) {
	dbDriver := "mysql"
	dbUser := "upsilon"
	dbPass := "upsilon"
	dbHost := "upsilon"

	connStr := dbUser + ":" + dbPass + "@tcp(" + dbHost + ":3306)/" + dbName

	log.Infof("conn str: %v", connStr)

	db, err := sql.Open(dbDriver, dbUser + ":" + dbPass + "@tcp(" + dbHost + ":3306)/" + dbName);

	if err != nil {
		panic(err.Error())
	}

	return db;
}

func GetMetricsFromResults(service ServiceResult) []Metric {
	if (strings.Contains(service.Output, "json")) {
		return extractMetrics(service.Output);
	} else {
		return make([]Metric, 0);
	}
}
