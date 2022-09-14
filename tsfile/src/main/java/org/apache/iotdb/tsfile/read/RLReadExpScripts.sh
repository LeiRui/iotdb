# 执行完下述脚本会生成：
# 1. RREPEAT个读TsFile耗时结果csv文件 *readResult-T*csv
# 2. 一个把重复读实验结果横向拼接起来的csv文件 *readResult-combined.csv
# 3. 一个把写结果和读结果拼接起来的csv文件 *allResult-combined.csv
# 4. 一个把读结果取平均值并且按照不同粒度统计百分比的csv文件 *allResult-combined-processed.csv

WRITE_READ_JAR_PATH=/disk/rl/tsfileReadExp/RLTsFileReadCostBench-0.13.1-jar-with-dependencies.jar

Calculator_JAR_PATH=/disk/rl/tsfileReadExp/RLRepeatReadResultAvgPercCalculator-0.13.1-jar-with-dependencies.jar

FILE_NAME=/disk/rl/tsfileReadExp/testTsFile/syn_ppn_10000_pic_10_cw_10_te_TS_2DIFF_vt_INT64_ve_RLE_co_SNAPPY

TSFILE=$FILE_NAME.tsfile

READ_RESULT=${FILE_NAME}*readResult-T*csv

decomposeMeasureTime=true

D_decompose_each_step=false

te=TS_2DIFF

REPEAT=5 # 重复次数

echo 3 | sudo tee /proc/sys/vm/drop_caches
sleep 3s

for((i=0;i<REPEAT;i++)) do
	echo 3 | sudo tee /proc/sys/vm/drop_caches
	sleep 3s
	java -jar $WRITE_READ_JAR_PATH READ $TSFILE $decomposeMeasureTime $D_decompose_each_step $te
	echo 3 | sudo tee /proc/sys/vm/drop_caches
	sleep 3s
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
#echo $ARGUMENTS
cut -d , -f $ARGUMENTS combined.csv > $FILE_NAME-${decomposeMeasureTime}-${D_decompose_each_step}-readResult-combined.csv
rm combined.csv

# 把写文件统计信息和读耗时结果写到一个csv文件里
cat $FILE_NAME*writeResult*csv > $FILE_NAME-${decomposeMeasureTime}-${D_decompose_each_step}-allResult-combined.csv
cat $FILE_NAME-${decomposeMeasureTime}-${D_decompose_each_step}-readResult-combined.csv >> $FILE_NAME-${decomposeMeasureTime}-${D_decompose_each_step}-allResult-combined.csv

# 统计各项平均耗时以及百分比、D1和D2内部各项平均耗时以及百分比
cp $FILE_NAME-${decomposeMeasureTime}-${D_decompose_each_step}-allResult-combined.csv $FILE_NAME-${decomposeMeasureTime}-${D_decompose_each_step}-allResult-combined-processed.csv
java -jar $Calculator_JAR_PATH $FILE_NAME-${decomposeMeasureTime}-${D_decompose_each_step}-allResult-combined-processed.csv

echo $WRITE_READ_JAR_PATH
echo $Calculator_JAR_PATH
echo $TSFILE
echo $decomposeMeasureTime
echo $D_decompose_each_step
echo $REPEAT
echo $READ_RESULT
echo $FILE_NAME-${decomposeMeasureTime}-${D_decompose_each_step}-readResult-combined.csv
echo $FILE_NAME-${decomposeMeasureTime}-${D_decompose_each_step}-allResult-combined.csv
echo $FILE_NAME-${decomposeMeasureTime}-${D_decompose_each_step}-allResult-combined-processed.csv


