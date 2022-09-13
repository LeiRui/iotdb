#!/bin/bash

############################
#script function
############################
setProperty(){
	  awk -v pat="^$1=" -v value="$1=$2" '{ if ($0 ~ pat) print value; else print $0; }' $3 > $3.tmp
	    mv $3.tmp $3
    }
############################
### usage: setProperty $key $value $filename
setProperty $1 $2 $3

### usage: setProperty $key $value $filename setProperty $1 $2 $3
# 使用示例： ./tool.sh enable_CPV true iotdb-engine-example.properties。要注意的是，好像这个指令只能修改properties文件中已经反注释的项。