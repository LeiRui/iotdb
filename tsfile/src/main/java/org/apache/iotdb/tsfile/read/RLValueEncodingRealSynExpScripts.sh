WRITE_READ_JAR_PATH=/disk/rl/tsfileReadExp/RLTsFileReadCostBench-0.13.1-jar-with-dependencies.jar
Calculator_JAR_PATH=/disk/rl/tsfileReadExp/RLRepeatReadResultAvgPercCalculator-0.13.1-jar-with-dependencies.jar
TOOL_PATH=/disk/rl/tsfileReadExp/RLtool.sh
READ_SCRIPT_PATH=/disk/rl/tsfileReadExp/RLReadExpScripts.sh

csv=/disk/rl/zc_data/ZT11529.csv
# 提取csv地址里的文件名
file_name="${csv##*/}"
file="${file_name%.*}"

for ve in PLAIN RLE TS_2DIFF GORILLA
do
  # 写数据参数
	ppn=10000
	pic=1000
	cw=10
	te=TS_2DIFF
	vt=DOUBLE
	co=SNAPPY
	# 读数据参数
	decomposeMeasureTime=TRUE
	D_decompose_each_step=FALSE
	REPEAT=10

	# 写数据
	java -jar $WRITE_READ_JAR_PATH write_real $csv $ppn $pic $te $vt $ve $co
	echo 3 | sudo tee /proc/sys/vm/drop_caches
	sleep 3s

	# 读数据
	FILE_NAME=/disk/rl/tsfileReadExp/testTsFile/${file}_ppn_${ppn}_pic_${pic}_te_${te}_vt_${vt}_ve_${ve}_co_${co}
	bash ${TOOL_PATH} WRITE_READ_JAR_PATH $WRITE_READ_JAR_PATH $READ_SCRIPT_PATH
	bash ${TOOL_PATH} Calculator_JAR_PATH $Calculator_JAR_PATH $READ_SCRIPT_PATH
	bash ${TOOL_PATH} FILE_NAME $FILE_NAME $READ_SCRIPT_PATH
	bash ${TOOL_PATH} decomposeMeasureTime $decomposeMeasureTime $READ_SCRIPT_PATH
	bash ${TOOL_PATH} D_decompose_each_step $D_decompose_each_step $READ_SCRIPT_PATH
	bash ${TOOL_PATH} te $te $READ_SCRIPT_PATH
	bash ${TOOL_PATH} REPEAT $REPEAT $READ_SCRIPT_PATH
	bash $READ_SCRIPT_PATH
done

# nohup ./xxx.sh 2>&1 &
# ps -ef | grep java
# ps -ef | grep java | awk '{print $2}' | xargs kill -9