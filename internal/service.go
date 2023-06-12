package metricgen;

import (
	log "github.com/sirupsen/logrus"
	"database/sql"
	upmodels "github.com/upsilonproject/upsilon-golib-database/pkg/models"
	updb "github.com/upsilonproject/upsilon-golib-database/pkg/database"
)

type MetricToGenerate struct {
	Service string
	Name string
}

func GetMetricsToGenerate(dbUpsilon *sql.DB) []MetricToGenerate {
	var sql string
	var ret = make([]MetricToGenerate, 0)

	sql = "SELECT service, name FROM to_generate ORDER BY service"
	cursorService, err := dbUpsilon.Query(sql)

	if err != nil {
		log.Errorf("Could not select services to generate %v", err)
		return ret;
	}

	for cursorService.Next() {
		var toGenerate = MetricToGenerate{}

		cursorService.Scan(&toGenerate.Service, &toGenerate.Name)

		ret = append(ret, toGenerate)
	}

	defer cursorService.Close();

	return ret
}

func getUnprocessedServiceResults(dbUpsilon *sql.DB) []upmodels.ServiceResult {
	var sql string
	var ret = make([]upmodels.ServiceResult, 0)

	sql = "UPDATE service_check_results SET metricProcessed = 1 WHERE id = ? "
	stmt, err := dbUpsilon.Prepare(sql)

	log.Debugf("Getting unprocessed service_check_results")

	sql = "SELECT r.id, r.output, r.checked, r.service FROM service_check_results r WHERE r.metricProcessed = 0 ORDER BY r.id ASC LIMIT 2000"
	cursor, err := dbUpsilon.Query(sql)

	if err != nil {
		panic(err)
	}

	for cursor.Next() {
		result := upmodels.ServiceResult{}

		cursor.Scan(&result.Id, &result.Output, &result.Updated, &result.Identifier)

		log.WithFields(log.Fields {
			"id": result.Id,
			"updated": result.Updated,
			"service": result.Identifier,
		}).Infof("New service_check_result to process")

		ret = append(ret, result)

		stmt.Exec(result.Id)
	}

	log.Infof("Total # of service_check_results to process: %v", len(ret))

	stmt.Close();

	return ret
}

func RunServiceLoop(dbUpsilon *sql.DB, stmtInsert *sql.Stmt) {
	results := getUnprocessedServiceResults(dbUpsilon)

	for _, wantedMetric := range GetMetricsToGenerate(dbUpsilon) {
		log.WithFields(log.Fields{
			"service": wantedMetric.Service,
			"metrics": wantedMetric.Name,
		}).Infof("Generating metrics for service")

		for _, result := range results {
			if result.Identifier != wantedMetric.Service {
				continue
			}

			metricsInResult := GetMetricsFromResult(result)

			metric, found := metricsInResult[wantedMetric.Name]

			if found {
				log.WithFields(log.Fields {
					"name": wantedMetric.Name,
					"result": result.Id,
				}).Infof("Found metric in result")

				updb.InsertMetric(stmtInsert, result, metric)
			} else {
				log.WithFields(log.Fields {
					"name": wantedMetric.Name,
					"result": result.Id,
					"service": result.Identifier,
				}).Warnf("Metric not found in result")
			}
		}
	}
}
