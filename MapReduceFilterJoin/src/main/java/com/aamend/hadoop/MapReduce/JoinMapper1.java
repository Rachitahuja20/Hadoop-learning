/*
 * Perform Join using MR
  Track file | artist file

I/P : A track file and an artist file
O/P : filter the record on the basis of seek and then output,
      Track_id, artist id and title of song
 *
 */

package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper1 extends Mapper<Object, Text, Text, Text> {

  private final int titleIndex = 3;
  private final int artistIndex = 2;
  private final int trackIndex = 0;

  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      return;
    }

    String[] recordSplits = line.toString().split(seperator);

    if (recordSplits.length == titleIndex + 1) {

      // Filter on the basis of seek
      Boolean containsSearchword =
          recordSplits[titleIndex].toLowerCase().contains(seek);

      String Trackid = recordSplits[trackIndex];
      String Artistname = recordSplits[artistIndex];
      String Title = recordSplits[titleIndex];

      // Filter
      if (containsSearchword)
        context.write(new Text(Trackid), new Text(Artistname + "\t" + Title));

    }
  }
}