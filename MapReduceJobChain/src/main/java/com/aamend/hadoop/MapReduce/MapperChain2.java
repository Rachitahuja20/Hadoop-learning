/*
Job Chaining: Filter records based on keyword A for Job A and 
then take that output as input for Job B
and filter for keyword B.

Job A -> Filter on keyword ("night")
Job B -> Filter on keyword ("black")
 */
package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperChain2 extends Mapper<Object, Text, Text, Text> {

  private final int trackidIndex = 0;
  private final int artistIndex = 2;
  private final int titleIndex = 3;

  String seek = "black";
  String seperator = "\t";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      return;
    }

    String[] recordSplits = line.toString().split(seperator);

    if (recordSplits.length == titleIndex + 1) {

      String Trackid = recordSplits[trackidIndex];
      String Artistname = recordSplits[artistIndex];
      String Title = recordSplits[titleIndex];

      Boolean containsSearchword = Title.toLowerCase().contains(seek);

      // Filter
      if (containsSearchword)
        context.write(new Text(Trackid), new Text(Trackid + "\t" + Artistname
            + "\t" + Title));

    }
  }
}