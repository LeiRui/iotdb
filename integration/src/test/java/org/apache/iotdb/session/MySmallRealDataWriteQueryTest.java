package org.apache.iotdb.session;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MySmallRealDataWriteQueryTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // * (1) min_time(%s), max_time(%s), first_value(%s), last_value(%s), min_value(%s), max_value(%s)
  //       => Don't change the sequence of the above six aggregates!
  // * (2) group by ([tqs,tqe),IntervalLength) => Make sure (tqe-tqs) is divisible by
  // IntervalLength!
  // * (3) NOTE the time unit of interval. Update for different datasets!
  /**
   * About why "Make sure (tqe-tqs) is divisible by IntervalLength!": The original groupBy in IoTDB
   * generates time spans by adding the unit length, for example, group by([0,10),3ms) generates
   * four time spans [0,3), [3,6), [6,9), [9,12). However, the semantic of M4 aggregation wants a
   * uniform division of [0,10). The solution here is to extend the tqe to make (tqe-tqs) is an
   * integer multiple of w.
   */
  private static final String queryFormat =
      "select min_time(%s), max_time(%s), first_value(%s), last_value(%s), min_value(%s), max_value(%s) "
          + "from %s "
          + "group by ([%d, %d), %d%s)"; // note the time precision unit is also parameterized

  private static final String queryFormat_UDF =
      "select M4(%1$s,'tqs'='%3$d','tqe'='%4$d','w'='%5$d') from %2$s where time>=%3$d and time<%4$d";

  private static String device = "root.game";
  private static String measurement = "s6";
  private static TSDataType tsDataType = TSDataType.INT64; // TSDataType.DOUBLE;
  private static String timestamp_precision = "ns"; // ns, us, ms
  private static long dataMinTime = 0;
  private static long dataMaxTime = 25599285703L;
  private static long total_time_length = dataMaxTime - dataMinTime;
  private static int total_point_number = 50000;

  // must be smaller than DeltaBinaryEncoder.BLOCK_DEFAULT_SIZE to fulfill the assumption that page=pack
  private static int iotdb_chunk_point_size = 100;

  private static long chunkAvgTimeLen =
      (long)
          Math.ceil(
              total_time_length / Math.ceil(total_point_number * 1.0 / iotdb_chunk_point_size));
  private static String filePath =
      "D:\\github\\321\\iotdb\\integration\\src\\test\\java\\org\\apache\\iotdb\\session\\BallSpeedSmallData.csv";
  private static int deletePercentage = 5; // 0 means no deletes. 0-100
  // deletePercentage是以chunks数量为标准的，比如说刷了899个chunks，10%的deletePercentage意味着mod文件里会有89条删除记录
  private static int deleteLenPercentage = 20; // 0-100 每次删除的时间长度，用chunkAvgTimeLen的百分比表示
  private static int timeIdx = 0; // 时间戳idx，从0开始
  private static int valueIdx = 1; // 值idx，从0开始
  private static int w = 3;
  private static long range = total_time_length;
  private static boolean enableRegularityTimeDecode = false;
  private static long regularTimeInterval = 511996L;
  private static M4Implementations approach = M4Implementations.M4_UDF;

  private static long seed = 20221011;
  private static Random random = new Random(seed);

  public enum M4Implementations {
    M4_UDF,
    M4_LSM
  }

  @Before
  public void setUp() throws Exception {
    // this is because the implementation of M4-LSM uses the assumption that chunk=page=pack
    Assert.assertTrue(DeltaBinaryEncoder.BLOCK_DEFAULT_SIZE + 1 >= iotdb_chunk_point_size);

    config.setTimestampPrecision(timestamp_precision);
    config.setAvgSeriesPointNumberThreshold(iotdb_chunk_point_size);
    config.setUnSeqTsFileSize(1073741824);
    config.setSeqTsFileSize(1073741824);
    //    config.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
    //    config.setEnableUnseqCompaction(false);
    config.setEnableSeqSpaceCompaction(false);
    config.setEnableUnseqSpaceCompaction(false);
    config.setEnableCrossSpaceCompaction(false);
    //    config.setEnablePerformanceStat(false);

    config.setEnableM4LSM(true);

    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(1073741824);

    // TODO
    //
    // TSFileDescriptor.getInstance().getConfig().setEnableRegularityTimeDecode(enableRegularityTimeDecode);
    //
    // TSFileDescriptor.getInstance().getConfig().setRegularTimeInterval(regularTimeInterval);

    EnvironmentUtils.envSetUp(); // start after configuration settings
    Class.forName(Config.JDBC_DRIVER_NAME);

    System.out.println("[WriteData] device=" + device);
    System.out.println("[WriteData] measurement=" + measurement);
    System.out.println("[WriteData] dataType=" + tsDataType);
    System.out.println("[WriteData] timestamp_precision=" + timestamp_precision);
    System.out.println("[WriteData] dataMinTime=" + dataMinTime);
    System.out.println("[WriteData] dataMaxTime=" + dataMaxTime);
    System.out.println("[WriteData] total_time_length=" + total_time_length);
    System.out.println("[WriteData] total_point_number=" + total_point_number);
    System.out.println("[WriteData] iotdb_chunk_point_size=" + iotdb_chunk_point_size);
    System.out.println("[WriteData] derived estimated chunkAvgTimeLen =" + chunkAvgTimeLen);
    System.out.println("[WriteData] filePath=" + filePath);
    System.out.println("[WriteData] deletePercentage=" + deletePercentage);
    System.out.println("[WriteData] deleteLenPercentage=" + deleteLenPercentage);
    System.out.println("[WriteData] timeIdx=" + timeIdx);
    System.out.println("[WriteData] valueIdx=" + valueIdx);
    // TODO
    //        System.out.println(
    //                "[WriteData] enableRegularityTimeDecode="
    //                        +
    // TSFileDescriptor.getInstance().getConfig().isEnableRegularityTimeDecode());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  /**
   * Before writing data, make sure check the server parameter configurations.
   */
  // Usage: java -jar WriteData-0.12.4.jar device measurement dataType timestamp_precision
  // total_time_length total_point_number iotdb_chunk_point_size filePath deleteFreq deleteLen
  // timeIdx valueIdx
  @Test
  public void test1() throws Exception {
    System.out.println("writing data...");
    writeData();

    System.out.println("querying data...");
    System.out.println("[QueryData] query range=" + range);
    System.out.println("[QueryData] w=" + w);

    //        if (!approach.equals("m4-udf") && !approach.equals("m4-lsm")) {
    //            throw new IOException("Approach wrong. Only accepts m4-udf/m4-lsm");
    //        }
    System.out.println("[QueryData] approach=" + approach);

    if (approach == M4Implementations.M4_LSM) {
      System.out.println(
          "MAKE SURE you have set the enable_m4lsm as true in `iotdb - engine.properties` for M4-LSM!");
      Assert.assertTrue(config.isEnableM4LSM());
    }

    long minTime;
    long maxTime;
    long interval;
    if (range >= (dataMaxTime - dataMinTime)) {
      minTime = dataMinTime;
      interval = (long) Math.ceil((double) (dataMaxTime - dataMinTime) / w);
    } else {
      // randomize between [dataMinTime, dataMaxTime-range]
      minTime =
          (long)
              Math.ceil(
                  dataMinTime + random.nextDouble() * (dataMaxTime - range - dataMinTime + 1));
      interval = (long) Math.ceil((double) range / w);
    }
    maxTime = minTime + interval * w;

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    // Set it big to avoid multiple fetch, which is very important.
    // Because the IOMonitor implemented in IoTDB does not cover the fetchResults operator yet.
    // As M4 already does data reduction, so even the w is very big such as 8000, the returned
    // query result size is no more than 8000*4=32000.
    session.setFetchSize(1000000);

    String sql;
    if (approach == M4Implementations.M4_UDF) {
      sql = String.format(queryFormat_UDF, measurement, device, minTime, maxTime, w);
    } else {
      sql =
          String.format(
              queryFormat,
              measurement,
              measurement,
              measurement,
              measurement,
              measurement,
              measurement,
              device,
              minTime,
              maxTime,
              interval,
              timestamp_precision); // note the time precision unit
    }

//    sql = "select * from " + device + " limit 10";

    System.out.println("[QueryData] sql=" + sql);

    long c = 0;
    SessionDataSet dataSet = session
        .executeQueryStatement(sql, 3600000);// set long timeout for debug
    DataIterator iterator = dataSet.iterator();
    System.out.println(dataSet.getColumnNames());
    while (iterator.next()) { // this way avoid constructing rowRecord
      c++;
      String ans;
      if (approach == M4Implementations.M4_UDF) {
        ans =
            String.format(
                "%s,%s",
                iterator.getString(1), // time
                iterator.getString(2)); // M4
      } else {
        ans =
            String.format(
                "%s,%s,%s,%s,%s,%s,%s",
                iterator.getString(1), // time
                iterator.getString(2), // min_time
                iterator.getString(3), // max_time
                iterator.getString(4), // first_value
                iterator.getString(5), // last_value
                iterator.getString(6), // min_value & bottomTime
                iterator.getString(7)); // max_value & topTime
      }
      System.out.println(ans);
    }
    //        Assert.assertEquals(w, c);
    System.out.println("[QueryData] query result line number=" + c);

    //    session.executeNonQueryStatement("clear cache");
    dataSet = session.executeFinish();

    // don't add more string to this output, as ProcessResult code depends on this.
    String info = dataSet.getFinishResult();
    System.out.println(info);

    dataSet.closeOperationHandle();
    session.close();
  }

  public void writeData()
      throws IoTDBConnectionException, StatementExecutionException, IOException {

    if (deletePercentage < 0 || deletePercentage > 100) {
      throw new IOException("WRONG deletePercentage!");
    }
    if (deleteLenPercentage < 0 || deleteLenPercentage > 100) {
      throw new IOException("WRONG deleteLenPercentage!");
    }

    // use period to control percentage
    int deletePeriod = (int) Math.floor(100 * 1.0 / deletePercentage * iotdb_chunk_point_size);
    long deleteLen = (long) Math.floor(chunkAvgTimeLen * deleteLenPercentage * 1.0 / 100);

    List<String> deletePaths = new ArrayList<>();
    deletePaths.add(device + "." + measurement);

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    // this is to make all following inserts unseq chunks
    if (timestamp_precision.toLowerCase().equals("ns")) {
      session.insertRecord(
          device,
          1644181628000000000L, // ns
          // NOTE UPDATE TIME DATATYPE! [[update]]. DONT USE System.nanoTime()!
          Collections.singletonList(measurement),
          Collections.singletonList(tsDataType), // NOTE UPDATE VALUE DATATYPE!
          parseValue("0", tsDataType)); // NOTE UPDATE VALUE DATATYPE!
    } else if (timestamp_precision.toLowerCase().equals("us")) {
      session.insertRecord(
          device,
          1644181628000000L, // us
          // NOTE UPDATE TIME DATATYPE! [[update]]. DONT USE System.nanoTime()!
          Collections.singletonList(measurement),
          Collections.singletonList(tsDataType), // NOTE UPDATE VALUE DATATYPE!
          parseValue("0", tsDataType)); // NOTE UPDATE VALUE DATATYPE!
    } else { // ms
      session.insertRecord(
          device,
          1644181628000L, // ms
          // NOTE UPDATE TIME DATATYPE! [[update]]. DONT USE System.nanoTime()!
          Collections.singletonList(measurement),
          Collections.singletonList(tsDataType), // NOTE UPDATE VALUE DATATYPE!
          parseValue("0", tsDataType)); // NOTE UPDATE VALUE DATATYPE!
    }
    session.executeNonQueryStatement("flush");

    File f = new File(filePath);
    String line = null;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    long lastDeleteMinTime = Long.MAX_VALUE;
    long lastDeleteMaxTime = Long.MIN_VALUE;
    int cnt = 0;
    while ((line = reader.readLine()) != null) {
      String[] split = line.split(",");
      long timestamp = Long.parseLong(split[timeIdx]);
      session.insertRecord(
          device,
          timestamp,
          Collections.singletonList(measurement),
          Collections.singletonList(tsDataType),
          parseValue(split[valueIdx], tsDataType));
      cnt++;

      if (timestamp > lastDeleteMaxTime) {
        lastDeleteMaxTime = timestamp;
      }
      if (timestamp < lastDeleteMinTime) {
        lastDeleteMinTime = timestamp;
      }

      if (deletePercentage != 0) {
        if (cnt >= deletePeriod) {
          cnt = 0;
          // randomize deleteStartTime in [lastMinTime, max(lastMaxTime-deleteLen,lastMinTime+1)]
          long rightBound = Math.max(lastDeleteMaxTime - deleteLen, lastDeleteMinTime + 1);
          long deleteStartTime =
              (long)
                  Math.ceil(
                      lastDeleteMinTime
                          + random.nextDouble() * (rightBound - lastDeleteMinTime + 1));
          long deleteEndTime = deleteStartTime + deleteLen - 1;
          session.deleteData(deletePaths, deleteStartTime, deleteEndTime);
          System.out.println("[[[[delete]]]]]" + deleteStartTime + "," + deleteEndTime);

          lastDeleteMinTime = Long.MAX_VALUE;
          lastDeleteMaxTime = Long.MIN_VALUE;
        }
      }
    }
    session.executeNonQueryStatement("flush");
    session.close();
  }

  public static Object parseValue(String value, TSDataType tsDataType) throws IOException {
    if (tsDataType == TSDataType.INT64) {
      return Long.parseLong(value);
    } else if (tsDataType == TSDataType.DOUBLE) {
      return Double.parseDouble(value);
    } else {
      throw new IOException("data type wrong");
    }
  }
}
