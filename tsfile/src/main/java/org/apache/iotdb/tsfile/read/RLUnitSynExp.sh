WRITE_READ_JAR_PATH=/disk/rl/tsfileReadExp/RLTsFileReadCostBench-0.13.1-jar-with-dependencies.jar
HOME_PATH=/disk/rl/tsfileReadExp

# 写人工数据参数
ppn=10000
pic=1000
cw=10
te=TS_2DIFF
vt=INT32
ve=PLAIN
co=SNAPPY

# 读数据参数
decomposeMeasureTime=TRUE
D_decompose_each_step=FALSE

cd $HOME_PATH
java -jar $WRITE_READ_JAR_PATH write_syn $ppn $pic $cw $te $vt $ve $co
echo 3 | sudo tee /proc/sys/vm/drop_caches
sleep 3s

# 写出来的TsFile地址
TSFILE=${HOME_PATH}/testTsFile/syn_ppn_${ppn}_pic_${pic}_cw_${cw}_te_${te}_vt_${vt}_ve_${ve}_co_${co}.tsfile

echo 3 | sudo tee /proc/sys/vm/drop_caches
sleep 3s
java -jar $WRITE_READ_JAR_PATH READ $TSFILE $decomposeMeasureTime $D_decompose_each_step $te



