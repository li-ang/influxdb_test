package main

import (
	"math/rand"
	"net/url"
	"strconv"
	"sync"
	"time"

	"qbox.us/cc/config"

	"github.com/influxdb/influxdb/client"

	qlog "github.com/qiniu/log.v1"
)

type Config struct {
	URL                     string            `json:"influxURL"`
	Database                string            `json:"database"`
	Retention               string            `json:"retention"`
	Precision               string            `json:"precision"`
	GorutineNum             int               `json:"gorutineNum"`
	BatchSize               int               `json:"batchSize"`
	BatchPointsInSameSeries bool              `json:"batchPointsInSameSeries"`
	BatchCountPerSecond     int               `json:"batchCountPerSecond"`
	DebugLevel              int               `json:"debugLevel"`
	Measurement             string            `json:"measurement`
	TagKeySet               []string          `json:"tagKeySet"`
	FieldKeySet             []string          `json:"fieldKeySet"`
	FieldValueRange         []FieldValueRange `json:"fieldValueRange"`
}

type FieldValueRange struct {
	Type  string `json:"type"`
	Start string `json:"start"`
	End   string `json:"end"`
}

func main() {

	config.Init("f", "", "benchmark.conf")

	var wg sync.WaitGroup
	var conf Config
	if err := config.Load(&conf); err != nil {
		qlog.Fatal("config.Load failed:", err)
		return
	}

	qlog.SetOutputLevel(conf.DebugLevel)

	host, err := url.Parse(conf.URL)

	if err != nil {
		return
	}

	var gorutineClients []*client.Client
	gorutineClients = make([]*client.Client, conf.GorutineNum)

	qlog.Debugf("The gorutineNum is %d", conf.GorutineNum)

	for i := 0; i < conf.GorutineNum; i++ {

		gorutineClients[i], err = client.NewClient(client.Config{URL: *host})
		if err != nil {
			return
		}

		wg.Add(1)
		go writePoints(i, gorutineClients[i], &conf)
	}
	wg.Wait()
}

func writePoints(num int, gorutineClient *client.Client, conf *Config) {

	batchCount := 1000 / conf.BatchCountPerSecond
	writeTimeDuration := (time.Duration(batchCount)) * time.Millisecond

	writeTimer := time.NewTimer(writeTimeDuration)

	batchSize := conf.BatchSize

	batchPointsInSameSeries := conf.BatchPointsInSameSeries

	tagKeySet := conf.TagKeySet
	fieldKeySet := conf.FieldKeySet

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		<-writeTimer.C

		pts := make([]client.Point, batchSize)

		for i := 0; i < batchSize; i++ {

			// tagSet := make(map[string]string, len(tagKeySet))
			tagSet := make(map[string]string, len(tagKeySet)+1)
			fieldSet := make(map[string]interface{}, len(fieldKeySet))

			for _, tag := range tagKeySet {
				tagSet[tag] = tag + "_" + strconv.Itoa(num)
			}

			if !batchPointsInSameSeries {
				for _, tag := range tagKeySet {
					tagSet[tag] = tag + strconv.Itoa(i)
				}
			}

			for index, val := range fieldKeySet {

				valueContent := conf.FieldValueRange[index]

				if valueContent.Type == "float" {
					start, _ := strconv.ParseFloat(conf.FieldValueRange[index].Start, 32)
					end, _ := strconv.ParseFloat(conf.FieldValueRange[index].End, 32)
					v := start + r.Float64()*(end-start)
					fieldSet[val] = v
				}

				if conf.FieldValueRange[index].Type == "integer" {
					start, _ := strconv.Atoi(conf.FieldValueRange[index].Start)
					end, _ := strconv.Atoi(conf.FieldValueRange[index].End)
					v := start + r.Intn(end-start)
					fieldSet[val] = v
				}

				if conf.FieldValueRange[index].Type == "string" {
				}

				if conf.FieldValueRange[index].Type == "bool" {
				}
			}

			pts[i] = client.Point{
				Measurement: conf.Measurement,
				Tags:        tagSet,
				Fields:      fieldSet,
				Time:        time.Now(),
				Precision:   conf.Precision,
			}
		}

		bp := client.BatchPoints{
			Points:          pts,
			Database:        conf.Database,
			RetentionPolicy: conf.Retention,
		}

		qlog.Debugf("gorutine number is %d, Write %d points", num, len(bp.Points))

		_, err := gorutineClient.Write(bp)

		if err != nil {
			qlog.Debug(err)
			return
		}

		writeTimer.Reset(writeTimeDuration)
	}

}
