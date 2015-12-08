#! /bin/zsh

echo "此脚本用/bin/zsh作为脚本解释器"

echo ""

echo "正在使用二进制版本启动InfluxDB"

echo ""

echo "是否删除旧数据: (yes or no)"

echo ""

read info

case $info in
	yes|y|Y)
	rm -rf $HOME/goproject/src/github.com/li-ang/influxdb_test/read_snapshot/.influxdb
	echo ""
	echo "已删除旧数据"
	;;

	no|n|N)
	echo ""
	echo "不删除旧数据"

	;;
esac

echo ""

cd $HOME/goproject/src/github.com/li-ang/influxdb_test/read_snapshot/

./influxd -config $HOME/goproject/src/github.com/li-ang/influxdb_test/read_snapshot/config.toml
