package main

import (
	"bytes"
	"fmt"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"

	qlog "github.com/qiniu/log.v1"
)

const PATH = "/Users/Leo/.influxdb/meta/raft.db"

func main() {
	db, err := raftboltdb.NewBoltStore(PATH)
	if err != nil {
		qlog.Info(err)
		return
	}

	lastIdx, err := db.LastIndex()

	if err != nil {
		qlog.Info(err)
		return
	}

	log := &raft.Log{}

	err = db.GetLog(lastIdx, log)

	if err != nil {
		qlog.Info(err)
		return
	}

	for i := 0; i <= int(lastIdx); i++ {
		if err = db.GetLog(lastIdx, log); err != nil {
			qlog.Debug(err)
			return
		}

		if log.Type == raft.LogAddPeer || log.Type == raft.LogCommand {
			// log.Data = encodePeers([]string{"192.168.1.3:8088"})
			peers := decodePeers(log.Data)

			if len(peers) == 0 {
				return
			}
			if PeerContained(peers, "localhost:8088") {
				log.Data = encodePeers([]string{"192.168.1.3:8088"})
				err = db.StoreLog(log)
				if err != nil {
					qlog.Debug(err)
					return
				}
			}
			err = db.StoreLog(log)
			if err != nil {
				qlog.Debug(err)
				return
			}
		}
	}
	return
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

// Decode reverses the encode operation on a byte slice input
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

func decodePeers(buf []byte) []string {

	var encPeers [][]byte
	if err := decodeMsgPack(buf, &encPeers); err != nil {
		panic(fmt.Errorf("failed to decode peers: %v", err))
	}

	var peers []string
	for _, enc := range encPeers {
		peers = append(peers, string(enc))
	}

	return peers
}

func PeerContained(peers []string, peer string) bool {
	for _, p := range peers {
		if p == peer {
			return true
		}
	}
	return false
}
