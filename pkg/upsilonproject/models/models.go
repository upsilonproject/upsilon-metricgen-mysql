package models;

type Metric struct {
	Name  string
	Value int64
}

type ServiceResult struct {
	Id         int
	Output     string
	Identifier string
	Updated    string
}

type MetricToGenerate struct {
	Service string
	Metrics []string
}
