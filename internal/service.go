package metricgen;

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"database/sql"
	upmodels "github.com/upsilonproject/upsilon-golib-database/pkg/models"
	updb "github.com/upsilonproject/upsilon-golib-database/pkg/database"
)

func GetMetricsToGenerate(dbUpsilon *sql.DB) []upmodels.MetricToGenerate {
	var sql string
	var ret = make([]upmodels.MetricToGenerate, 0)

	sql = "SELECT DISTINCT service FROM to_generate ORDER BY service"
	cursorService, err := dbUpsilon.Query(sql)

	if err != nil {
		log.Errorf("Could not select services to generate %v", err)
		return ret;
	}

	for cursorService.Next() {
		var toGenerate = upmodels.MetricToGenerate{Service: "ignore", Metrics: make([]string, 0)}

		cursorService.Scan(&toGenerate.Service)

		sql = "SELECT name FROM to_generate WHERE service = ?"
		cursorNames, _ := dbUpsilon.Query(sql, toGenerate.Service)

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

func getUnprocessedServiceResults(dbUpsilon *sql.DB, serviceName string) []upmodels.ServiceResult {
	var sql string
	var ret = make([]upmodels.ServiceResult, 0)

	sql = "UPDATE service_check_results SET metricProcessed = 1 WHERE id = ? "
	stmt, err := dbUpsilon.Prepare(sql)

	log.Infof("Getting unprocessed services")

	sql = "SELECT r.id, r.output, r.checked, r.service FROM service_check_results r WHERE r.service = ? AND r.metricProcessed = 0 ORDER BY r.id ASC LIMIT 2000"
	cursor, err := dbUpsilon.Query(sql, serviceName)

	if err != nil {
		panic(err)
	}

	for cursor.Next() {
		result := upmodels.ServiceResult{}

		cursor.Scan(&result.Id, &result.Output, &result.Updated, &result.Identifier)

		fmt.Printf("id: %v, updated %v \n", result.Id, result.Updated)

		ret = append(ret, result)

		stmt.Exec(result.Id)
	}

	stmt.Close();

	log.Infof("Finished getting service results relevant for metric")

	return ret
}

func RunServiceLoop(dbUpsilon *sql.DB, stmtInsert *sql.Stmt) {
	for _, metricsToGenerate := range GetMetricsToGenerate(dbUpsilon) {
		log.Infof("Generating metrics for service", metricsToGenerate.Service)
		log.Infof("togen: %v", metricsToGenerate)

		results := getUnprocessedServiceResults(dbUpsilon, metricsToGenerate.Service)

		for _, result := range results {
			log.Infof("Checking result: %v", result.Id)

			for _, usefulMetric := range metricsToGenerate.Metrics {
				log.Infof("Useful metric: %v", usefulMetric)

				for _, metric := range GetMetricsFromResults(result) {
					if (metric.Name == usefulMetric) {
						updb.InsertMetric(stmtInsert, result, metric)
					}
				}
			}
		}
	}
}
