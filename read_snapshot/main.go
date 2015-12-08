package main

import (
	// "archive/tar"
	// "errors"
	"encoding/json"
	"fmt"
	// "bytes"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/golang/snappy"

	// "github.com/influxdb/influxdb/meta"
	// "github.com/influxdb/influxdb/snapshot"
	"github.com/influxdb/influxdb/tsdb"

	"github.com/qiniu/log.v1"
)

const (
	TSMFileExtension = "tsm1"

	IDsFileExtension = "ids"

	FieldsFileExtension = "fields"

	SeriesFileExtension = "series"
)

func main() {

	path := "/Users/Leon/goproject/src/github.com/li-ang/influxdb_test/read_snapshot/.influxdb/data/"
	db := "foo"
	rp := "default"
	shardID := "1"

	readTSMFile(path, db, rp, shardID)
}

func readTSMFile(path string, db string, rp string, shardID string) {
	path = filepath.Join(path, db, rp, shardID)
	tsmFilesPath, _ := filepath.Glob(filepath.Join(path, fmt.Sprintf("*.%s", TSMFileExtension)))
	// idsPath := filepath.Join(path, IDsFileExtension)
	fieldsPath := filepath.Join(path, FieldsFileExtension)
	seriesPath := filepath.Join(path, SeriesFileExtension)

	fields, _ := readFields(fieldsPath)
	series, _ := readSeries(seriesPath)

	log.Info("~~~~~~~~~~~~~fields~~~~~~~~~~~~~~~~~~~")
	for k, v := range fields {
		log.Info(k)
		log.Info(v.Fields)
	}

	log.Info("~~~~~~~~~~~~~series~~~~~~~~~~~~~~~~~~~")
	for k, v := range series {
		log.Info(k)
		log.Info(v.Tags)
		log.Info(v.Key)
	}

	for _, v := range tsmFilesPath {

		log.Info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		readDataFile(v)
	}

}

func readDataFile(path string) {
	magicNumber := make([]byte, 4)
	version := make([]byte, 4)

	f, err := os.Open(path)
	defer f.Close()

	if err != nil {
		log.Info(err)
		return
	}

	f.Read(magicNumber)

	f.Read(version)
	// log.Infof("%s", magicNumber)
	log.Info(magicNumber)
	log.Info(version)
}

func readFields(path string) (map[string]*tsdb.MeasurementFields, error) {
	fields := make(map[string]*tsdb.MeasurementFields)

	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	defer f.Close()
	if os.IsNotExist(err) {
		return fields, nil
	} else if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}

	return fields, nil
}

func readSeries(path string) (map[string]*tsdb.Series, error) {
	series := make(map[string]*tsdb.Series)

	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	defer f.Close()
	if os.IsNotExist(err) {
		return series, nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &series); err != nil {
		return nil, err
	}

	return series, nil
}

func readCompressedFile(path string) (map[string]uint64, error) {
	ids := make(map[string]uint64)

	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return ids, err
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return ids, err
	}

	data, err := snappy.Decode(nil, b)
	if err != nil {
		return ids, err
	}
	if data != nil {
		if err := json.Unmarshal(data, &ids); err != nil {
			return ids, err
		}
	}
	return ids, nil
}

func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func u32tob(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func btou32(b []byte) uint32 {
	return uint32(binary.BigEndian.Uint32(b))
}

// func unpack(path string) error {

// 	mr, files, err := snapshot.OpenFileMultiReader(path)
// 	if err != nil {
// 		return fmt.Errorf("open multireader: %s", err)
// 	}

// 	defer closeAll(files)

// 	for {
// 		sf, err := mr.Next()
// 		if err == io.EOF {
// 			break
// 		} else if err != nil {
// 			return fmt.Errorf("next: entry=%s, err=%s", sf.Name, err)
// 		}

// 		fmt.Fprintf(os.Stdout, "unpacking: %s (%d bytes)\n", sf.Name, sf.Size)

// 		switch sf.Name {
// 		case "meta":
// 			if err := cmd.unpackMeta(mr, sf); err != nil {
// 				return fmt.Errorf("meta: %s", err)
// 			}
// 		default:
// 			if err := cmd.unpackData(mr, sf); err != nil {
// 				return fmt.Errorf("data: %s", err)
// 			}
// 		}
// 	}

// 	return nil
// }

// func unpackMeta(mr *snapshot.MultiReader, sf snapshot.File, folder string) error {
// 	var buf bytes.Buffer

// 	if _, err := io.CopyN(&buf, mr, sf.Size); err != nil {
// 		return fmt.Errorf("copy: %s", err)
// 	}

// 	var data meta.Data
// 	if err := data.UnmarshalBinary(buf.Bytes()); err != nil {
// 		return fmt.Errorf("unmarshal: %s", err)
// 	}

// 	return nil
// }

// func unpackData(mr *snapshot.MultiReader, sf snapshot.File, folder string) error {
// 	path := filepath.Join(folder, sf.Name)

// 	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
// 		return fmt.Errorf("mkdir: entry=%s, err=%s", sf.Name, err)
// 	}

// 	f, err := os.Create(path)
// 	if err != nil {
// 		return fmt.Errorf("create: entry=%s, err=%s", sf.Name, err)
// 	}
// 	defer f.Close()

// 	if _, err := io.CopyN(f, mr, sf.Size); err != nil {
// 		return fmt.Errorf("copy: entry=%s, err=%s", sf.Name, err)
// 	}

// 	return nil
// }

// func closeAll(a []io.Closer) {
// 	for _, c := range a {
// 		_ = c.Close()
// 	}
// }
