// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package influxdb

import (
	"encoding/json"
	"fmt"
	"github.com/fcddk/remote-storage-adapter/config"
	"math"
	"os"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/fcddk/remote-storage-adapter/prompb"
)

// Client allows sending batches of Prometheus samples to InfluxDB.
type Client struct {
	logger log.Logger

	client          influx.Client
	database        string
	retentionPolicy string
	ignoredSamples  prometheus.Counter
	receiveSamples  prometheus.Counter
	sendSamples     prometheus.Counter
	adapter         *adapterManager
	databases       map[string]*databasesManager
}

// NewClient creates a new Client.
func NewClient(logger log.Logger, conf influx.HTTPConfig, db string, rp string, adapterConf *config.Config) *Client {
	c, err := influx.NewHTTPClient(conf)
	// Currently influx.NewClient() *should* never return an error.
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	if logger == nil {
		logger = log.NewNopLogger()
	}

	cli := &Client{
		logger:          logger,
		client:          c,
		database:        db,
		retentionPolicy: rp,
		ignoredSamples: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "prometheus_influxdb_adapter_ignored_samples_total",
				Help: "The total number of samples not sent to InfluxDB due to unsupported float values (Inf, -Inf, NaN), or ignored by checking measurement.",
			},
		),
		receiveSamples: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "prometheus_influxdb_adapter_receive_samples_total",
				Help: "The total number of samples prometheus remote write.",
			},
		),
		sendSamples: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "prometheus_influxdb_adapter_send_samples_total",
				Help: "The total number of samples adapter send to influxdb.",
			},
		),
	}

	//init adapter
	ada := cli.createAdapterManager(adapterConf)
	if ada == nil {
		level.Error(logger).Log("err", fmt.Errorf("adapter manager is nil"))
		os.Exit(1)
	}
	cli.adapter = ada
	level.Info(cli.logger).Log("msg", "adapter", "config:", ada.String())

	// init databases
	cli.createDatabasesManager(logger)
	return cli
}

// tagsFromMetric extracts InfluxDB tags from a Prometheus metric.
func tagsFromMetric(m model.Metric) map[string]string {
	tags := make(map[string]string, len(m)-1)
	for l, v := range m {
		if l != model.MetricNameLabel {
			tags[string(l)] = string(v)
		}
	}
	return tags
}

func (c *Client) tagsOrFieldFromMetric(m model.Metric, measurementName string) (map[string]string, map[string]string) {
	tags := make(map[string]string)
	fields := make(map[string]string)
	measurementObj, ok := c.adapter.measurements[measurementName]
	if ok {
		for l, v := range m {
			if l != model.MetricNameLabel {
				_, hasOk := measurementObj.Tags[string(l)]
				if hasOk {
					tags[string(l)] = string(v)
				} else {
					_, in := measurementObj.DropLabels[string(l)]
					if in {
						continue
					}
					fields[string(l)] = string(v)
				}
			}
		}
	}
	return tags, fields
}

// tagsFromMetric extracts InfluxDB tags from a Prometheus metric.
func tagsOrFieldFromMetric(m model.Metric) (map[string]string, map[string]string) {
	tags := make(map[string]string, len(m)-1)
	fields := make(map[string]string, len(m)-1)
	for l, v := range m {
		if l != model.MetricNameLabel {
			_, hasOk := tagsWhitelist[string(l)]
			if hasOk {
				tags[string(l)] = string(v)
			} else {
				fields[string(l)] = string(v)
			}
		}
	}
	return tags, fields
}

func (c *Client) createDatabasesManager(logger log.Logger) {
	if c.adapter == nil {
		level.Error(logger).Log("err", fmt.Errorf("adapter manager is nil"))
		os.Exit(1)
	}
	c.databases = map[string]*databasesManager{}
	for name, measure := range c.adapter.measurements {
		if len(measure.Fields) == 0 {
			_, hasOk := c.databases[measure.Database]
			if !hasOk {
				c.databases[measure.Database] = &databasesManager{
					name:         measure.Database,
					metrics:      map[string]string{},
					measurements: map[string]*measurement{},
				}
			}
			_, existMeasure := c.databases[measure.Database].measurements[name]
			if !existMeasure {
				c.databases[measure.Database].measurements[name] = measure
			}
			metricName := name
			_, existMetric := c.databases[measure.Database].metrics[metricName]
			if !existMetric {
				c.databases[measure.Database].metrics[metricName] = name
			}
		}
		for field, _ := range measure.Fields {
			_, hasOk := c.databases[measure.Database]
			if !hasOk {
				c.databases[measure.Database] = &databasesManager{
					name:         measure.Database,
					metrics:      map[string]string{},
					measurements: map[string]*measurement{},
				}
			}
			_, existMeasure := c.databases[measure.Database].measurements[name]
			if !existMeasure {
				c.databases[measure.Database].measurements[name] = measure
			}
			metricName := name + "_" + field
			_, existMetric := c.databases[measure.Database].metrics[metricName]
			if !existMetric {
				c.databases[measure.Database].metrics[metricName] = name
			}
		}
	}
	level.Debug(logger).Log("msg", "process databases data", "databases:%v", c.getDatabasesInfo())
}

func (c *Client) getDatabasesInfo() string {
	var info string
	info = "["
	for _, m := range c.databases {
		info = info + m.String() + ","
	}
	info = info + "]"
	return info
}

func (c *Client) createAdapterManager(conf *config.Config) *adapterManager {
	adapterM := &adapterManager{}
	measurementList := make(map[string]*measurement)
	adapterM.measurements = measurementList
	for _, meas := range conf.GlobalConfig.MeasurementsWhitelist {
		_, hasOk := adapterM.measurements[meas]
		if hasOk {
			continue
		}
		measurementOne := &measurement{
			Name:       meas,
			Tags:       map[string]bool{},
			Database:   c.database,
			Fields:     nil,
			DropLabels: nil,
		}
		for _, tag := range conf.GlobalConfig.TagsWhitelist {
			measurementOne.Tags[tag] = true
		}
		adapterM.measurements[meas] = measurementOne
	}

	for _, measConf := range conf.MeasurementsConfig {
		if measConf.Name == "" {
			level.Error(c.logger).Log("measurement name is empty")
			continue
		}
		measurementOne := &measurement{
			Name:       measConf.Name,
			Tags:       map[string]bool{},
			Database:   measConf.Database,
			Fields:     map[string]bool{},
			DropLabels: map[string]bool{},
		}

		if measurementOne.Database == "" {
			measurementOne.Database = c.database
		}
		for _, tag := range conf.GlobalConfig.TagsWhitelist {
			measurementOne.Tags[tag] = true
		}
		for _, tagElem := range measConf.Tags {
			measurementOne.Tags[tagElem] = true
		}

		for _, fieldElem := range measConf.Fields {
			measurementOne.Fields[fieldElem] = true
		}

		for _, label := range measConf.DropLabels {
			measurementOne.DropLabels[label] = true
		}
		adapterM.measurements[measConf.Name] = measurementOne
	}

	return adapterM
}

// Write sends a batch of samples to InfluxDB via its HTTP API.
func (c *Client) Write(samples model.Samples) error {
	start := time.Now()
	points := make(map[string][]*influx.Point)
	c.receiveSamples.Add(float64(len(samples)))
	for _, s := range samples {
		v := float64(s.Value)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			level.Debug(c.logger).Log("msg", "Cannot send  to InfluxDB, skipping sample", "value", v, "sample", s)
			c.ignoredSamples.Inc()
			continue
		}
		tags := make(map[string]string)
		fields := make(map[string]interface{})
		//castrate metric name
		//measure := hasMeasurement(string(s.Metric[model.MetricNameLabel]))
		//measure, fieldOne := castrate.CastrateMetricName(measure, string(s.Metric[model.MetricNameLabel]))
		measure, fieldOne := c.checkSampleBelongToMeasurement(string(s.Metric[model.MetricNameLabel]))
		if measure == "" {
			level.Debug(c.logger).Log("msg", "metric", s.Metric[model.MetricNameLabel], "measurement is nil")
			level.Info(c.logger).Log("msg", "check measurement", " time_consume:", time.Since(start))
			c.ignoredSamples.Inc()
			continue
		}
		level.Info(c.logger).Log("msg", "check measurement", " time_consume:", time.Since(start))
		level.Debug(c.logger).Log("msg", "info", "metric", s.Metric[model.MetricNameLabel], "measurement", measure)
		labelStart := time.Now()
		//metricTags, metricFields := tagsOrFieldFromMetric(s.Metric)
		metricTags, metricFields := c.tagsOrFieldFromMetric(s.Metric, measure)
		if len(metricTags) == 0 {
			level.Info(c.logger).Log("msg", "metric", s.Metric[model.MetricNameLabel], "tags is nil")
			c.ignoredSamples.Inc()
			continue
		}
		tags = metricTags
		for l, v := range metricFields {
			fields[l] = v
		}
		if fieldOne == "" {
			fields["value"] = v
		} else {
			fields[fieldOne] = v
		}
		p, err := influx.NewPoint(
			measure,
			tags,
			fields,
			s.Timestamp.Time(),
		)
		level.Info(c.logger).Log("msg", "process tags and fields", " time_consume:", time.Since(labelStart))
		//p, err := influx.NewPoint(
		//	string(s.Metric[model.MetricNameLabel]),
		//	tagsFromMetric(s.Metric),
		//	map[string]interface{}{"value": v},
		//	s.Timestamp.Time(),
		//)
		if err != nil {
			return err
		}
		// todo nil error
		//if c.adapter == nil {
		//}
		_, pOk := points[c.adapter.measurements[measure].Database]
		if !pOk {
			points[c.adapter.measurements[measure].Database] = make([]*influx.Point, 0)
		}
		points[c.adapter.measurements[measure].Database] = append(points[c.adapter.measurements[measure].Database], p)
		//points = append(points, p)
	}

	//bps, err := influx.NewBatchPoints(influx.BatchPointsConfig{
	//	Precision:       "ms",
	//	Database:        c.database,
	//	RetentionPolicy: c.retentionPolicy,
	//})
	//if err != nil {
	//	return err
	//}
	//bps.AddPoints(points)
	c.sendSamples.Add(float64(len(points)))
	for name, pointsJob := range points {
		bps, err := influx.NewBatchPoints(influx.BatchPointsConfig{
			Precision:       "ms",
			Database:        name,
			RetentionPolicy: c.retentionPolicy,
		})
		if err != nil {
			return err
		}
		bps.AddPoints(pointsJob)
		err = c.client.Write(bps)
		if err != nil {
			level.Error(c.logger).Log("msg", "failed to write once", "err", err.Error())
		}
	}
	level.Info(c.logger).Log("msg", "write points", "num:", len(samples), " time_consume:", time.Since(start))
	return nil
}

func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	labelsToSeries := map[string]*prompb.TimeSeries{}
	for _, q := range req.Queries {
		command, err := c.buildCommand(q)
		if err != nil {
			return nil, err
		}

		query := influx.NewQuery(command, c.database, "ms")
		resp, err := c.client.Query(query)
		if err != nil {
			return nil, err
		}
		if resp.Err != "" {
			return nil, errors.New(resp.Err)
		}

		if err = mergeResult(labelsToSeries, resp.Results); err != nil {
			return nil, err
		}
	}

	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{Timeseries: make([]*prompb.TimeSeries, 0, len(labelsToSeries))},
		},
	}
	for _, ts := range labelsToSeries {
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
	}
	return &resp, nil
}

func (c *Client) buildCommand(q *prompb.Query) (string, error) {
	matchers := make([]string, 0, len(q.Matchers))
	// If we don't find a metric name matcher, query all metrics
	// (InfluxDB measurements) by default.
	from := "FROM /.+/"
	for _, m := range q.Matchers {
		if m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				from = fmt.Sprintf("FROM %q.%q", c.retentionPolicy, m.Value)
			case prompb.LabelMatcher_RE:
				from = fmt.Sprintf("FROM %q./^%s$/", c.retentionPolicy, escapeSlashes(m.Value))
			default:
				// TODO: Figure out how to support these efficiently.
				return "", errors.New("non-equal or regex-non-equal matchers are not supported on the metric name yet")
			}
			continue
		}

		switch m.Type {
		case prompb.LabelMatcher_EQ:
			matchers = append(matchers, fmt.Sprintf("%q = '%s'", m.Name, escapeSingleQuotes(m.Value)))
		case prompb.LabelMatcher_NEQ:
			matchers = append(matchers, fmt.Sprintf("%q != '%s'", m.Name, escapeSingleQuotes(m.Value)))
		case prompb.LabelMatcher_RE:
			matchers = append(matchers, fmt.Sprintf("%q =~ /^%s$/", m.Name, escapeSlashes(m.Value)))
		case prompb.LabelMatcher_NRE:
			matchers = append(matchers, fmt.Sprintf("%q !~ /^%s$/", m.Name, escapeSlashes(m.Value)))
		default:
			return "", errors.Errorf("unknown match type %v", m.Type)
		}
	}
	matchers = append(matchers, fmt.Sprintf("time >= %vms", q.StartTimestampMs))
	matchers = append(matchers, fmt.Sprintf("time <= %vms", q.EndTimestampMs))

	return fmt.Sprintf("SELECT value %s WHERE %v GROUP BY *", from, strings.Join(matchers, " AND ")), nil
}

func escapeSingleQuotes(str string) string {
	return strings.Replace(str, `'`, `\'`, -1)
}

func escapeSlashes(str string) string {
	return strings.Replace(str, `/`, `\/`, -1)
}

func mergeResult(labelsToSeries map[string]*prompb.TimeSeries, results []influx.Result) error {
	for _, r := range results {
		for _, s := range r.Series {
			k := concatLabels(s.Tags)
			ts, ok := labelsToSeries[k]
			if !ok {
				ts = &prompb.TimeSeries{
					Labels: tagsToLabelPairs(s.Name, s.Tags),
				}
				labelsToSeries[k] = ts
			}

			samples, err := valuesToSamples(s.Values)
			if err != nil {
				return err
			}

			ts.Samples = mergeSamples(ts.Samples, samples)
		}
	}
	return nil
}

func concatLabels(labels map[string]string) string {
	// 0xff cannot occur in valid UTF-8 sequences, so use it
	// as a separator here.
	separator := "\xff"
	pairs := make([]string, 0, len(labels))
	for k, v := range labels {
		pairs = append(pairs, k+separator+v)
	}
	return strings.Join(pairs, separator)
}

func tagsToLabelPairs(name string, tags map[string]string) []prompb.Label {
	pairs := make([]prompb.Label, 0, len(tags))
	for k, v := range tags {
		if v == "" {
			// If we select metrics with different sets of labels names,
			// InfluxDB returns *all* possible tag names on all returned
			// series, with empty tag values on series where they don't
			// apply. In Prometheus, an empty label value is equivalent
			// to a non-existent label, so we just skip empty ones here
			// to make the result correct.
			continue
		}
		pairs = append(pairs, prompb.Label{
			Name:  k,
			Value: v,
		})
	}
	pairs = append(pairs, prompb.Label{
		Name:  model.MetricNameLabel,
		Value: name,
	})
	return pairs
}

func valuesToSamples(values [][]interface{}) ([]prompb.Sample, error) {
	samples := make([]prompb.Sample, 0, len(values))
	for _, v := range values {
		if len(v) != 2 {
			return nil, errors.Errorf("bad sample tuple length, expected [<timestamp>, <value>], got %v", v)
		}

		jsonTimestamp, ok := v[0].(json.Number)
		if !ok {
			return nil, errors.Errorf("bad timestamp: %v", v[0])
		}

		jsonValue, ok := v[1].(json.Number)
		if !ok {
			return nil, errors.Errorf("bad sample value: %v", v[1])
		}

		timestamp, err := jsonTimestamp.Int64()
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert sample timestamp to int64")
		}

		value, err := jsonValue.Float64()
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert sample value to float64")
		}

		samples = append(samples, prompb.Sample{
			Timestamp: timestamp,
			Value:     value,
		})
	}
	return samples, nil
}

// mergeSamples merges two lists of sample pairs and removes duplicate
// timestamps. It assumes that both lists are sorted by timestamp.
func mergeSamples(a, b []prompb.Sample) []prompb.Sample {
	result := make([]prompb.Sample, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Timestamp < b[j].Timestamp {
			result = append(result, a[i])
			i++
		} else if a[i].Timestamp > b[j].Timestamp {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// Name identifies the client as an InfluxDB client.
func (c Client) Name() string {
	return "influxdb"
}

// Describe implements prometheus.Collector.
func (c *Client) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ignoredSamples.Desc()
}

// Collect implements prometheus.Collector.
func (c *Client) Collect(ch chan<- prometheus.Metric) {
	ch <- c.ignoredSamples
}
