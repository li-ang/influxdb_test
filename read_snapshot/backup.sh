#! /bin/zsh

echo "此脚本用/bin/zsh作为脚本解释器"

echo ""

echo "开始进行备份，备份文件在snapshots文件夹"

echo ""

cd $HOME/goproject/src/github.com/li-ang/influxdb_test/read_snapshot/

./influxd backup $HOME/goproject/src/github.com/li-ang/influxdb_test/read_snapshot/snapshots/backup.tar

echo ""

cd $HOME/goproject/src/github.com/li-ang/influxdb_test/read_snapshot/snapshots

echo "全部的备份文件为："

ls