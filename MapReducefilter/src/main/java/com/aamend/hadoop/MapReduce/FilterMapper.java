/*
MR job for Filter the records for the songs with title containing "night"

O/P : Output record which has only records with title containing keyword.
 */
package com.aamend.hadoop.MapReduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<Object, Text, Text, Text> {

  private final int trackidIndex = 0;
  private final int artistIndex = 2;
  private final int titleIndex = 3;

  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      return;
    }

    String[] recordSplits = line.toString().split(seperator);

    // Checking if the length of recordSplits is exactly what we are finding
    if (recordSplits.length == titleIndex + 1) {

      // Searching for the keyword
      Boolean containsSearchword =
          recordSplits[titleIndex].toLowerCase().contains(seek);

      String Trackid = recordSplits[trackidIndex];
      String Artistname = recordSplits[artistIndex];
      String Title = recordSplits[titleIndex];

      // Map if keyword found
      if (containsSearchword)
        context.write(new Text(Trackid), new Text(Artistname + "\t" + Title));

    }
  }
}
