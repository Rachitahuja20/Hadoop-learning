package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper1 extends Mapper<Object, Text, Text, Text> {

  private final int StateIndex = 3;
  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    String[] splits = line.toString().split(seperator);

    if (splits.length == StateIndex + 1) {

      // Filter on the basis of seek
      Boolean containsSearchword =
          splits[StateIndex].toLowerCase().contains(seek);

      String Trackid = splits[StateIndex - 3];
      String Artistname = splits[StateIndex - 1];
      String Title = splits[StateIndex];

      // Filter
      if (containsSearchword)
        context.write(new Text(Trackid), new Text(Artistname + "\t" + Title));

    }
  }
}