# 执行完下述脚本会生成：
# 1. a个读取耗时结果csv文件
# 2. 一个把上述结果横向拼接起来的csv文件

JAR_PATH=/disk/rl/tsfileReadExp/RLTestChunkReadCostWithRealDataSet-0.13.1-jar-with-dependencies.jar
echo $JAR_PATH

FILE_NAME=/disk/rl/tsfileReadExp/testTsFile/syn_ppn_10000_pic_10_cw_10_te_TS_2DIFF_vt_INT64_ve_RLE_co_SNAPPY

TSFILE=$FILE_NAME.tsfile
echo $TSFILE

READ_RESULT=$FILE_NAME*readResult*csv
echo $READ_RESULT

decomposeMeasureTime=true
D_decompose_each_step=false

REPEAT=5 # 重复次数

echo 3 | sudo tee /proc/sys/vm/drop_caches
sleep 3s

for((i=0;i<REPEAT;i++)) do
	echo 3 | sudo tee /proc/sys/vm/drop_caches
	sleep 3s
	java -jar $JAR_PATH READ $TSFILE $decomposeMeasureTime $D_decompose_each_step
	echo 3 | sudo tee /proc/sys/vm/drop_caches
	sleep 2s
done

# 把a个结果文件的结果排在一个csv里面
# 竖向：cat *readResult*csv > combined.csv
# 横向：
paste -d, $READ_RESULT > combined.csv
# 最后只保留需要的列：
ARGUMENTS="1"
for((i=1;i<REPEAT+1;i++)) do
	j=$((i*2))
	ARGUMENTS+=","
	ARGUMENTS+=$j
done
echo $ARGUMENTS
cut -d , -f $ARGUMENTS combined.csv > $FILE_NAME-readResult-combined.csv
rm combined.csv
echo $FILE_NAME-readResult-combined.csv

# 把写文件统计信息和读耗时结果写到一个csv文件里
cat $FILE_NAME*writeResult*csv > $FILE_NAME-allResult-combined.csv
cat $FILE_NAME-readResult-combined.csv >> $FILE_NAME-allResult-combined.csv
echo $FILE_NAME-allResult-combined.csv