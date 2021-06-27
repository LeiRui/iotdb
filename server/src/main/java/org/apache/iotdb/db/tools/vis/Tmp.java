//package org.apache.iotdb.db.tools.vis;
//
//import java.io.IOException;
//import java.util.List;
//import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
//
//public class Tmp {
//
//  public static void main(String[] args) throws IOException {
//
//    String file =
//        "E:\\kobelco神钢\\2021-0618_root.kobelco.trans.34\\root.kobelco.trans.34\\0\\626\\1623930762240-103-0-0.tsfile";
//    ;
//    try (TsFileSequenceReader reader = new TsFileSequenceReader(file)) {
//      reader.selfCheck()
//
//      // get all ChunkMetadatas
//      List<String> devices = reader.getAllDevices();
//    }
//  }
//
//}
