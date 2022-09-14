WRITE_READ_JAR_PATH=/disk/rl/tsfileReadExp/RLTsFileReadCostBench-0.13.1-jar-with-dependencies.jar
HOME_PATH=/disk/rl/tsfileReadExp

# 写人工数据参数
csv=/disk/rl/zc_data/ZT11529.csv
ppn=10000
pic=1000
te=TS_2DIFF
vt=DOUBLE
ve=GORILLA
co=SNAPPY

# 读数据参数
decomposeMeasureTime=TRUE
D_decompose_each_step=FALSE

cd $HOME_PATH
java -jar $WRITE_READ_JAR_PATH write_real $csv $ppn $pic $te $vt $ve $co
echo 3 | sudo tee /proc/sys/vm/drop_caches
sleep 3s

# 提取csv地址里的文件名
file_name="${csv##*/}"
file="${file_name%.*}"
# 写出来的TsFile地址
TSFILE=${HOME_PATH}/testTsFile/${file}_ppn_${ppn}_pic_${pic}_te_${te}_vt_${vt}_ve_${ve}_co_${co}.tsfile

echo 3 | sudo tee /proc/sys/vm/drop_caches
sleep 3s
java -jar $WRITE_READ_JAR_PATH READ $TSFILE $decomposeMeasureTime $D_decompose_each_step $te



