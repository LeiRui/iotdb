/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.edu.tsinghua.tsfile.format;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

/**
 * Supported compression algorithms.
 */
public enum CompressionType implements org.apache.thrift.TEnum {
  UNCOMPRESSED(0),
  SNAPPY(1),
  GZIP(2),
  LZO(3),
  SDT(4),
  PAA(5),
  PLA(6);

  private final int value;

  private CompressionType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static CompressionType findByValue(int value) { 
    switch (value) {
      case 0:
        return UNCOMPRESSED;
      case 1:
        return SNAPPY;
      case 2:
        return GZIP;
      case 3:
        return LZO;
      case 4:
        return SDT;
      case 5:
        return PAA;
      case 6:
        return PLA;
      default:
        return null;
    }
  }
}
