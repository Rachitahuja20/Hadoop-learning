package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper2 extends Mapper<Object, Text, Text, Text> {

  private final int StateIndex = 3;
  private final int artistIndex = 0;
  private final int trackIndex = 2;

  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      return;
    }

    String[] recordSplits = line.toString().split(seperator);

    if (recordSplits.length == StateIndex + 1) {

      String Artid1 = recordSplits[artistIndex];
      String Trackid = recordSplits[trackIndex];

      context.write(new Text(Trackid), new Text(Artid1));

    }
  }
}