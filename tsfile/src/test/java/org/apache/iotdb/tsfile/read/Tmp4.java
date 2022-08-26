package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.Assert;

import java.io.File;

public class Tmp4 {

  public static void main(String[] args) throws Exception {
    // ==============write tsfile==============
    final String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
    File file = new File(filePath);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }

    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    tsFileConfig.setMaxNumberOfPointsInPage(6); // set small pages
    tsFileConfig.setGroupSizeInByte(100 * 1024 * 1024);
    TsFileWriter tsFileWriter = new TsFileWriter(file, new Schema(), tsFileConfig);
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(2);

    Path mypath;
    for (int i = 1; i <= 5; i++) {
      for (int j = 1; j <= 5; j++) {
        mypath = new Path("d" + i, "s" + j);
        tsFileWriter.registerTimeseries(
            new Path(mypath.getDevice()),
            new MeasurementSchema(
                mypath.getMeasurement(), TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZ4));
      }
    }

    for (int n = 0; n < 10; n++) {
      for (int i = 1; i <= 5; i++) {
        TSRecord t = new TSRecord(n, "d" + i);
        for (int j = 1; j <= 5; j++) {
          t.addTuple(new IntDataPoint("s" + j, n));
        }
        tsFileWriter.write(t);
      }
    }
    tsFileWriter.flushAllChunkGroups();
    tsFileWriter.close();
  }
}
