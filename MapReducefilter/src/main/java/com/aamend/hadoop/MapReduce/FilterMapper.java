package com.aamend.hadoop.MapReduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<Object, Text, Text, Text> {

  private final int StateIndex = 3;
  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    String[] recordSplits = line.toString().split(seperator);

    if (recordSplits.length == StateIndex + 1) {

      // Searching for the keyword seek
      Boolean containsSearchword =
          recordSplits[StateIndex].toLowerCase().contains(seek);

      String Trackid = recordSplits[StateIndex - 3];
      String Artistname = recordSplits[StateIndex - 1];
      String Title = recordSplits[StateIndex];

      // Filter for the keyword
      if (containsSearchword)
        context.write(new Text(Trackid), new Text(Artistname + "\t" + Title));

    }
  }
}
