package metricgen;

import (
	"errors"
	"strings"
	"fmt"
	"regexp"
	"encoding/json"
	"github.com/tidwall/gjson"

	. "github.com/upsilonproject/upsilon-golib-database/pkg/models"
)

func findJsonInOutput(output string) (string, error) {
	r := regexp.MustCompile("(?s)<json>(.*)<\\/json>");
	groups := r.FindAllStringSubmatch(output, -1);

	jsonOutput := groups[0][1]

	isValid := json.Valid([]byte(jsonOutput))

	if (!isValid) {
		return "", errors.New("JSON is invalid");
	}

	return jsonOutput, nil
}

func parseMetric(output gjson.Result) Metric {
	var m = Metric{};
	m.Name = output.Get("name").String();
	m.Value = output.Get("value").Int();

	return m;
}

func extractMetricsJson(output string) (map[string]Metric) {
	var ret = make(map[string]Metric, 0);
	jsonOutput, err := findJsonInOutput(output);

	if (err != nil) {
		fmt.Println(err);
	}

	gjson.Get(jsonOutput, "metrics").ForEach(
		func(_, v gjson.Result) bool {
			metric := parseMetric(v)

			ret[metric.Name] = metric;

			return true;
		},
	)

	return ret;
}

func extractMetrics(output string) (map[string]Metric) {
	ret := make(map[string]Metric)

	if (strings.Contains(output, "<json>")) {
		ret = extractMetricsJson(output);
	} else {
		ret = make(map[string]Metric, 0);
	}

	return ret;
}
