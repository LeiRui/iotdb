# M4-LSM 
- The code of three M4-LSM deployments, MA, MO and MF, is available in this [GitHub repository](https://github.com/apache/iotdb/tree/research/M4-visualization).
    - MA: `org.apache.iotdb.db.query.udf.builtin.UDTFM4MAC`.  
    The document of the M4 aggregation function (implemented as a UDF based on MA) has been released on the official [website](https://iotdb.apache.org/UserGuide/Master/UDF-Library/M4.html#m4-2) of Apache IoTDB.
    - MO: `org.apache.iotdb.db.query.dataset.groupby.LocalGroupByExecutor`
    - MF: `org.apache.iotdb.db.query.dataset.groupby.LocalGroupByExecutor4CPV`
    - Some integration tests for correctness are in `org.apache.iotdb.db.integration.m4.MyTest1/2/3/4`.
- The experiment-related code, data and scripts are in [another GitHub repository](https://github.com/LeiRui/M4-visualization-exp) for easier reproducibility.
- For the README of Apache IoTDB itself, please see [README_IOTDB.md](README_IOTDB.md).

## 技术方法方面

**查询执行整体耗时->解码步骤整体耗时->解码步骤里的loadIntBatch累计耗时**

**主要问题：**

1.   **【内部】M4-LSMloadIntBatch累计耗时使用新方法没有加速**
     1.   **LongToBytes：op1/op2/...**
     2.   **Compare Bytes 改进：1%**
     3.   **BytesToLong 改进：10%**
2.   **【外部】虽然解码步骤整体耗时是查询执行整体耗时的瓶颈，但是解码步骤里的loadIntBatch累计耗时并不是解码步骤整体耗时的瓶颈！哪怕删掉了一些filter判断和数据类型判断**



### 分支research/M4-visualization-backup20221012-encodeRegularBytesPerPack

-   【内部】op1
    -   目标：加速loadIntBatch累计耗时（timeColumnTS2DIFFLoadBatchCost）
    -   编解码改造：已经把regularBytes写到每一个pack里，这样解码的时候不用专门生成，而是直接读出来。
    -   涉及主要类：DeltaBinaryEncoder, DeltaBinaryDecoder
    -   TODO问题：
        -   空间膨胀还是比较大的
        -   如果是现在这样子，在编码写数据的时候就把regularBytes写到每一个pack里，那似乎有其他更方便的做法，比如对等于regular delta的位置保存bitmap。
        -   而且这个方法需要预备知识，且写了就定了，不如解码的时候再提供可以改动的regular time interval信息来得灵活
-   【外部】
    -   目标：尽可能减少解码步骤里loadIntBatch之外的其它操作耗时，从而使loadIntBatch的加速作用凸显一些，换言之尽可能让loadIntBatch成为解码步骤的耗时瓶颈。否则现在的情况是：尽管解码步骤占整体查询耗时占比最大，但是解码步骤里面的loadIntBatch累计耗时占比很小，于是即便loadIntBatch极端加速到零耗时，对整体耗时的加速也不明显。
    -   基于chunk=page=pack的假设（所以在TS_2DIFF DEFAULT BLOACK SIZE=128个点per pack的固定参数下，写数据用的chunk_point_size=100没有超过128是满足这个假设的），1）把timeDecoder.hasNext&timeDecoder.readLong改成了直接取出data array来消费；2）把能去掉的filter check去掉；3）把数值列的数据量类型switch判断也去掉了，因为实验数据集都是long类型的数值列。
    -   涉及主要类：
        -   M4-UDF: PageReader.getAllSatisfiedBatchData
        -   M4-LSM: PageReader.split4CPV, PageReader.partialScan
            -   pageReader.split4CPV：负责当候选点因为M4 time span/删除/更新而失效而要去update的时候的update。它会遍历这个page里的点，对取出来的点进行删除和过滤条件判断，并且按照M4 time spans拆分，变成落进相应的span里的batchData和chunkmetadata
                -   这个过程中，解读出来的batchData就会缓存下来
            -   pageReader.partialScan：负责BP/TP的candidate update verification，对与候选点有时间重叠且版本更高的块，它会从前到后逐点判断，直到找到等于候选点时间戳的点，或者直接大于候选点时间戳说明不存在更新。
                -   这个过程中，只涉及到timeDecoder读取的那一个pack出来的timeData数组会被缓存下来

### 分支research/M4-visualization-backup20221012-onlyChangeDecoder 

-   【内部】op2: 只改造解码，不动编码。不去改变编码写数据的流程，只是在解码的时候使用用户额外给的regular time interval信息来加速解码
    -   TODO问题：至于解码的时候需要生成regular bytes这个步骤，可以考虑通过全局缓存cache hit来减小代价（之前那种一个page里缓存allRegularBytes行不通，因为page=pack的假设下根本没有二次hit的机会。）
-   【外部】无改动
