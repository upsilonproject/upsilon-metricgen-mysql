package metricgen;

import (
	"fmt"
	"log"
	"database/sql"
	updb "github.com/upsilonproject/upsilon-golib-database/pkg/database/"
)

func GetMetricsToGenerate(dbMetrics *sql.DB) []MetricToGenerate {
	var sql string
	var ret = make([]MetricToGenerate, 0)

	sql = "SELECT DISTINCT service FROM to_generate ORDER BY service"
	cursorService, _ := dbMetrics.Query(sql)

	for cursorService.Next() {
		var toGenerate = MetricToGenerate{Service: "ignore", Metrics: make([]string, 0)}

		cursorService.Scan(&toGenerate.Service)

		sql = "SELECT name FROM to_generate WHERE service = ?"
		cursorNames, _ := dbMetrics.Query(sql, toGenerate.Service)

		for cursorNames.Next() {
			currentMetric := ""

			cursorNames.Scan(&currentMetric);

			toGenerate.Metrics = append(toGenerate.Metrics, currentMetric);
		}

		defer cursorNames.Close()

		ret = append(ret, toGenerate)
	}

	defer cursorService.Close();

	return ret
}

func getUnprocessedServiceResults(dbUpsilon *sql.DB, serviceName string) []ServiceResult {
	var sql string
	var ret = make([]ServiceResult, 0)

	sql = "UPDATE service_check_results SET metricProcessed = 1 WHERE id = ? "
	stmt, err := dbUpsilon.Prepare(sql)

	log.Println("Getting unprocessed services")

	sql = "SELECT r.id, r.output, r.checked, r.service FROM service_check_results r WHERE r.service = ? AND r.metricProcessed = 0 ORDER BY r.id ASC LIMIT 2000"
	cursor, err := dbUpsilon.Query(sql, serviceName)

	if err != nil {
		panic(err)
	}

	for cursor.Next() {
		result := ServiceResult{}

		cursor.Scan(&result.Id, &result.Output, &result.Updated, &result.Identifier)

		fmt.Printf("id: %v, updated %v \n", result.Id, result.Updated)

		ret = append(ret, result)

		stmt.Exec(result.Id)
	}

	stmt.Close();

	log.Println("Finished getting service results relevant for metric")

	return ret
}

func RunServiceLoop(dbUpsilon *sql.DB, dbMetrics *sql.DB, stmtInsert *sql.Stmt) {
	for _, metricsToGenerate := range GetMetricsToGenerate(dbMetrics) {
		log.Println("Generating metrics for service", metricsToGenerate.Service)
		log.Println(metricsToGenerate)

		results := getUnprocessedServiceResults(dbUpsilon, metricsToGenerate.Service)

		for _, result := range results {
			fmt.Println("Checking result:", result.Id)

			for _, usefulMetric := range metricsToGenerate.Metrics {
				log.Println("Useful metric:", usefulMetric)

				for _, metric := range GetMetricsFromResults(result) {
					if (metric.Name == usefulMetric) {
						updb.InsertMetric(stmtInsert, result, metric)
					}
				}
			}
		}
	}
}
