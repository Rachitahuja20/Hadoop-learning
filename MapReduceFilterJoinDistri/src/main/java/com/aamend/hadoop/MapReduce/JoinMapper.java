/*
 * 
    Perform Join using MR using Distributed Cache
    Track file | artist file

  I/P : A track file and an artist file as distributed cache (Smaller file as cache).
  O/P : filter the record on the basis of seek and then output,
        Track_id, artist id and title of song
 *
 */

package com.aamend.hadoop.MapReduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<Object, Text, Text, Text> {

  Path[] cachefiles = new Path[0]; // To store the path of lookup files

  // TODO use dictionary (hashmap) instead of array list
  List<String> Artists = new ArrayList<String>();// To store the data of lookup
                                                 // files

  @Override
  public void setup(Context context)

  {
    Configuration conf = context.getConfiguration();

    try {

      cachefiles = DistributedCache.getLocalCacheFiles(conf);

      BufferedReader reader =
          new BufferedReader(new FileReader(cachefiles[0].toString()));

      String cacheLine;

      while ((cacheLine = reader.readLine()) != null) {

        Artists.add(cacheLine); // Data of lookup files get stored in list
                                // object
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

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

      String Trackid = recordSplits[trackIndex];
      String Artistname = recordSplits[artistIndex];
      String Title = recordSplits[titleIndex];

      // TODO remove this O(N) loop and use O(1) check instead
      // O(1) means a call to method like myHashmap.Contains(foo)
      for (String e : Artists) {

        String[] listLine = e.toString().split(seperator);

        if (listLine.length > 0) {
          String Track = listLine[artistIndex];

          if (Trackid.equals(Track)) {
            context.write(new Text(Trackid), new Text(listLine[trackIndex]
                + "\t"
                + Artistname + "\t" + Title));

          }
        }
      }
    }
  }
}