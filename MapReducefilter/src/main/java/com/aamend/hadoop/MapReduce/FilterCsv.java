package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterCsv extends Mapper<Object, Text, Text, Text> {

  // For CSV file StateProvince.cs

  private final int StateIndex = 1;

  private Text word = new Text();

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    String[] csv = line.toString().split("\t");

    if (csv.length > 1) {

      // "   apple and banana " => "apple and banana"
      String trimmed = csv[StateIndex].trim();

      byte[] foo = trimmed.getBytes();

      byte[] bar = new byte[2];
      bar[0] = foo[0];
      bar[1] = foo[2];

      // char[] t = csv[StateIndex].trim().toCharArray();
      //
      // String trimmed = new String(t);

      String bytes = trimmed + "   ";

      // trimmed.getBytes("US-ASCII")
      // for (Byte b : new String(bar)
      // .getBytes("US-ASCII")) {
      // bytes += String.format("0x%02X ", b);
      // }
      //
      // bytes += "  >>>>> ";
      // for (Byte b : new String("CA".getBytes(), "US-ASCII")
      // .getBytes("US-ASCII")) {
      // bytes += String.format("0x%02X ", b);
      // }
      if (new String(bar).compareToIgnoreCase("CA") == 0) {

        // if (trimmed == "CA") {
        context.write(new Text(), new Text(line));
      }
    }
  }
}
