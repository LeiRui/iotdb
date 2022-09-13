JAR_PATH=/disk/rl/tsfileReadExp/RLTestChunkReadCostWithRealDataSet-0.13.1-jar-with-dependencies.jar
TOOL_PATH=/disk/rl/tsfileReadExp/tool.sh
READ_SCRIPT_PATH=/disk/rl/tsfileReadExp/RLReadExpScripts.sh

for co in UNCOMPRESSED SNAPPY GZIP LZ4
do
	ppn=10000
	pic=1000
	cw=10
	te=TS_2DIFF
	vt=INT64
	ve=PLAIN
	java -jar $JAR_PATH write_syn $ppn $pic $cw $te $vt $ve $co
	echo 3 | sudo tee /proc/sys/vm/drop_caches
	sleep 3s
	FILE_NAME=/disk/rl/tsfileReadExp/testTsFile/syn_ppn_${ppn}_pic_${pic}_cw_${cw}_te_${te}_vt_${vt}_ve_${ve}_co_${co}
	bash ${TOOL_PATH} JAR_PATH $JAR_PATH $READ_SCRIPT_PATH
	bash ${TOOL_PATH} FILE_NAME $FILE_NAME $READ_SCRIPT_PATH
	bash ${TOOL_PATH} decomposeMeasureTime TRUE $READ_SCRIPT_PATH
	bash ${TOOL_PATH} D_decompose_each_step FALSE $READ_SCRIPT_PATH
	bash ${TOOL_PATH} REPEAT 5 $READ_SCRIPT_PATH
	bash $READ_SCRIPT_PATH
done

# nohup ./xxx.sh 2>&1 &
# ps -ef | grep java
# ps -ef | grep java | awk '{print $2}' | xargs kill -9