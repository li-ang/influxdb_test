package main

import (
	"bytes"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/influxdb/influxdb/meta/internal"

	qlog "github.com/qiniu/log.v1"
)

const (
	PATH = "/Users/Leon/Desktop/influxdb-node-1/10.1.0.197/p0/meta/raft.db"
)

func main() {
	qlog.SetOutputLevel(0)
	db, err := raftboltdb.NewBoltStore(PATH)
	lastIdx, err := db.LastIndex()
	firtIdx, err := db.FirstIndex()
	for i := int(firtIdx); i <= int(lastIdx); i++ {
		log := new(raft.Log)
		if err = db.GetLog(uint64(i), log); err != nil {
			qlog.Debug(err)
			continue
		}

		switch log.Type {
		case raft.LogCommand:
			qlog.Info("The raftlog type is LogCommand")
			var cmd internal.Command
			if err := proto.Unmarshal(log.Data, &cmd); err != nil {
				qlog.Debug(err)
				continue
			}
			command := cmd.GetType()
			qlog.Infof("The command is: %s", command)
			if command == internal.Command_CreateNodeCommand {
				ext, _ := proto.GetExtension(&cmd, internal.E_CreateNodeCommand_Command)
				v, ok := ext.(*internal.CreateNodeCommand)
				if !ok {
					continue
				}
				qlog.Debug("v.GetHost()", v.GetHost())
			}

			if command == internal.Command_CreateDatabaseCommand {
				ext, _ := proto.GetExtension(&cmd, internal.E_CreateDatabaseCommand_Command)
				v, ok := ext.(*internal.CreateDatabaseCommand)
				if !ok {
					continue
				}
				qlog.Debug("v.GetName()", v.GetName())
				qlog.Debug("v.String()", v.String())
			}

			if command == internal.Command_CreateRetentionPolicyCommand {
				ext, _ := proto.GetExtension(&cmd, internal.E_CreateRetentionPolicyCommand_Command)
				v, ok := ext.(*internal.CreateRetentionPolicyCommand)
				if !ok {
					continue
				}
				qlog.Debug("v.GetDatabase()", v.GetDatabase())
				qlog.Debug("v.GetRetentionPolicy()", v.GetRetentionPolicy())
				qlog.Debug("v.String()", v.String())
			}

			if command == internal.Command_SetDefaultRetentionPolicyCommand {
				ext, _ := proto.GetExtension(&cmd, internal.E_SetDefaultRetentionPolicyCommand_Command)
				v, ok := ext.(*internal.SetDefaultRetentionPolicyCommand)
				if !ok {
					continue
				}
				qlog.Debug("v.GetDatabase()", v.GetDatabase())
				qlog.Debug("v.GetName()", v.GetName())
				qlog.Debug("v.String()", v.String())
			}

		case raft.LogNoop:
			qlog.Info("The raftlog type is LogNoop")
		case raft.LogAddPeer:
			qlog.Info("The raftlog type is LogAddPeer")
			peers := decodePeers(log.Data)

			if len(peers) == 0 {
				qlog.Debug("peers == 0")
				continue
			}
			qlog.Infof("peers is:%s", peers)

		case raft.LogRemovePeer:
			qlog.Info("The raftlog type is LogAddPeer")
		}
	}
	return
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
