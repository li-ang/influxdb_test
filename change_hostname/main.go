package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"

	qlog "github.com/qiniu/log.v1"
	"qbox.us/cc/config"
)

const (
	jsonPeers = "peers.json"
	dbRaft    = "raft.db"
)

type Config struct {
	Dir    string `json: "Dir"`
	Origin string `json: "Origin"`
	New    string `json: "New"`
}

func main() {
	qlog.SetOutputLevel(0)
	config.Init("f", "", "default.conf")

	conf := &Config{}
	if err := config.Load(conf); err != nil {
		qlog.Fatal("config.Load failed:", err)
		return
	}

	peersPath := filepath.Join(conf.Dir, jsonPeers)
	raftPath := filepath.Join(conf.Dir, dbRaft)

	if err := modifyPeers(conf.Origin, conf.New, peersPath); err != nil {
		return
	}

	if err := modifyRaftDB(conf.Origin, conf.New, raftPath); err != nil {
		return
	}

	return
}

func modifyPeers(originPeer string, newPeer string, path string) error {
	bufRead, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if len(bufRead) == 0 {
		return fmt.Errorf("%s file is empty", path)
	}

	var originPeers []string
	dec := json.NewDecoder(bytes.NewReader(bufRead))
	if err := dec.Decode(&originPeers); err != nil {
		return err
	}

	if len(originPeers) != 1 || originPeers[0] != originPeer {
		return fmt.Errorf("%s file content is not % s", path, originPeer)
	}

	newPeers := []string{newPeer}

	var bufWrite bytes.Buffer
	enc := json.NewEncoder(&bufWrite)
	if err := enc.Encode(newPeers); err != nil {
		return err
	}

	return ioutil.WriteFile(path, bufWrite.Bytes(), 0755)
}

func modifyRaftDB(originPeer string, newPeer string, path string) error {
	db, err := raftboltdb.NewBoltStore(path)
	if err != nil {
		qlog.Debug(err)
		return err
	}

	lastIdx, err := db.LastIndex()

	if err != nil {
		qlog.Debug(err)
		return err
	}

	for i := 1; i < int(lastIdx); i++ {
		log := new(raft.Log)
		if err = db.GetLog(uint64(i), log); err != nil {
			qlog.Debug(err)
			continue
		}

		if log.Type == raft.LogAddPeer || log.Type == raft.LogCommand {
			peers := decodePeers(log.Data)

			if len(peers) == 0 {
				continue
			}

			if peerContained(peers, originPeer) {
				log.Data = encodePeers([]string{newPeer})
				err = db.StoreLog(log)
				if err != nil {
					qlog.Debug(err)
					return err
				}
			}
		}
	}
	return nil
}

func encodePeers(peers []string) []byte {

	var encPeers [][]byte
	for _, p := range peers {
		encPeers = append(encPeers, []byte(p))
	}

	buf, err := encodeMsgPack(encPeers)
	if err != nil {
		panic(fmt.Errorf("failed to encode peers: %v", err))
	}

	return buf.Bytes()
}

func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

func decodePeers(buf []byte) []string {

	var encPeers [][]byte
	if err := decodeMsgPack(buf, &encPeers); err != nil {
		return nil
	}

	var peers []string
	for _, enc := range encPeers {
		peers = append(peers, string(enc))
	}

	return peers
}

func peerContained(peers []string, peer string) bool {
	for _, p := range peers {
		if p == peer {
			return true
		}
	}
	return false
}
