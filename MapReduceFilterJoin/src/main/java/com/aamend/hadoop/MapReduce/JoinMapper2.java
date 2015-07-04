package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper2 extends Mapper<Object, Text, Text, Text> {

  private final int StateIndex = 3;
  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    String[] splits = line.toString().split(seperator);

    if (splits.length == StateIndex + 1) {

      String Artid1 = splits[StateIndex - 3];
      String Trackid = splits[StateIndex - 1];

      context.write(new Text(Trackid), new Text(Artid1));

    }
  }
}