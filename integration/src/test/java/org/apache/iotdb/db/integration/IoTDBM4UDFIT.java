package org.apache.iotdb.db.integration;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import org.apache.iotdb.commons.udf.builtin.UDTFM4;
import org.apache.iotdb.db.query.udf.example.ExampleUDFConstant;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBM4UDFIT {

  @BeforeClass
  public static void setUp() throws Exception {
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(5)
        .setUdfTransformerMemoryBudgetInMB(5)
        .setUdfReaderMemoryBudgetInMB(5);
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(100)
        .setUdfTransformerMemoryBudgetInMB(100)
        .setUdfReaderMemoryBudgetInMB(100);
  }

  @Test
  public void test1() {
    int timeInterval = 2;
    int slidingStep = 2;
    int displayWindowBegin = 0;
    int displayWindowEnd = 7;

    String sql =
        String.format(
            "select accumulator(s1, '%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s') from root.vehicle.d1",
            ExampleUDFConstant.ACCESS_STRATEGY_KEY,
            ExampleUDFConstant.ACCESS_STRATEGY_SLIDING_TIME,
            ExampleUDFConstant.TIME_INTERVAL_KEY,
            timeInterval,
            ExampleUDFConstant.SLIDING_STEP_KEY,
            slidingStep,
            ExampleUDFConstant.DISPLAY_WINDOW_BEGIN_KEY,
            displayWindowBegin,
            ExampleUDFConstant.DISPLAY_WINDOW_END_KEY,
            displayWindowEnd);

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);

      System.out.println(resultSet.getMetaData());

      while (resultSet.next()) {
        System.out.println(resultSet.getString(1) + "," + resultSet.getString(2));
      }
    } catch (SQLException throwable) {
      if (slidingStep > 0 && timeInterval > 0 && displayWindowEnd >= displayWindowBegin) {
        fail(throwable.getMessage());
      }
    }
  }

  @Test
  public void test2() {

    String sql =
        "select EQUAL_SIZE_BUCKET_M4_SAMPLE(s1,'proportion'='0.9') from root.vehicle.d1";

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);

      System.out.println(resultSet.getMetaData());

      while (resultSet.next()) {
        System.out.println(resultSet.getString(1) + "," + resultSet.getString(2));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void test3() {

    String sql = String.format("select M4(s1, '%s'='%s','%s'='%s','%s'='%s','%s'='%s') from root.vehicle.d1",
        UDTFM4.DISPLAY_WINDOW_BEGIN_KEY,
        0,
        UDTFM4.DISPLAY_WINDOW_END_KEY,
        100,
        UDTFM4.TIME_INTERVAL_KEY,
        25,
        UDTFM4.SLIDING_STEP_KEY,
        25
    );

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);

      while (resultSet.next()) {
        System.out.println(resultSet.getString(1) + "," + resultSet.getString(2));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void test4() {

    String sql = String.format("select M4(s1,'%s'='%s','%s'='%s') from root.vehicle.d1",
        UDTFM4.WINDOW_SIZE_KEY,
        10,
        UDTFM4.SLIDING_STEP_KEY,
        10
    );

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);

      while (resultSet.next()) {
        System.out.println(resultSet.getString(1) + "," + resultSet.getString(2));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.vehicle");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d1(timestamp,s1)" + " VALUES(%d,%d)";

  private static String[] creationSqls =
      new String[]{
          "SET STORAGE GROUP TO root.vehicle.d0",
          "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT64",
      };

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function counter as 'org.apache.iotdb.db.query.udf.example.Counter'");
      statement.execute(
          "create function accumulator as 'org.apache.iotdb.db.query.udf.example.Accumulator'");
      statement.execute(
          "create function time_window_tester as 'org.apache.iotdb.db.query.udf.example.SlidingTimeWindowConstructionTester'");
      statement.execute(
          "create function size_window_0 as 'org.apache.iotdb.db.query.udf.example.SlidingSizeWindowConstructorTester0'");
      statement.execute(
          "create function size_window_1 as 'org.apache.iotdb.db.query.udf.example.SlidingSizeWindowConstructorTester1'");
      statement.execute(
          "create function window_start_end as 'org.apache.iotdb.db.query.udf.example.WindowStartEnd'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
// data:
    // https://user-images.githubusercontent.com/33376433/151985070-73158010-8ba0-409d-a1c1-df69bad1aaee.png
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 25, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 3));
//      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 27, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 40));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 35, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 33, 9));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 45, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 52, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 18));
      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }


}
