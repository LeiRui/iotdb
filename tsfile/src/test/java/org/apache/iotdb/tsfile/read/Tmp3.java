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

public class Tmp3 {

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

    Path mypath = new Path("t", "id");
    tsFileWriter.registerTimeseries(
        new Path(mypath.getDevice()),
        new MeasurementSchema("id", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZ4));
    Path mypath2 = new Path("t", "id2");
    tsFileWriter.registerTimeseries(
        new Path(mypath2.getDevice()),
        new MeasurementSchema("id2", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZ4));
    Path mypath3 = new Path("t2", "a");
    tsFileWriter.registerTimeseries(
        new Path(mypath3.getDevice()),
        new MeasurementSchema("a", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZ4));

    for (int i = 0; i < 10; i++) {
      TSRecord t = new TSRecord(i, "t");
      t.addTuple(new IntDataPoint("id", i));
      t.addTuple(new IntDataPoint("id2", i));
      tsFileWriter.write(t);

      TSRecord t2 = new TSRecord(i, "t2");
      t2.addTuple(new IntDataPoint("a", i));
      tsFileWriter.write(t2);
    }
    tsFileWriter.flushAllChunkGroups();
    tsFileWriter.close();
  }
}
