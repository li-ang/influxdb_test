#! /bin/zsh

echo "此脚本用/bin/zsh作为脚本解释器"

echo ""

echo "正在启动InfluxDB"

echo ""

echo "注意，由于是在同一台机器中启动三个InfluxDB进程，因此部分端口已经改变，具体细节请查看配置文件"

echo ""

echo "当前InfluxDB的分支为："

echo ""

cd $HOME/goproject/src/github.com/influxdb/influxdb/

echo "git branch -a"

echo ""

git branch -a

echo "是否删除旧数据: (yes or no)"

echo ""

read info

case $info in
	yes|y|Y)
	rm -rf $HOME/goproject/src/github.com/li-ang/influxdb_test/cluster/.three
	echo ""
	echo "已删除旧数据"
	echo ""
	cd $HOME/goproject/src/github.com/influxdb/influxdb/cmd/influxd/
	go run main.go -config $HOME/goproject/src/github.com/li-ang/influxdb_test/cluster/three.toml -join 127.0.0.1:8088
	;;

	no|n|N)
	echo ""
	echo "不删除旧数据"
	echo ""
	cd $HOME/goproject/src/github.com/influxdb/influxdb/cmd/influxd/
	go run main.go -config $HOME/goproject/src/github.com/li-ang/influxdb_test/cluster/three.toml

	;;
esac




