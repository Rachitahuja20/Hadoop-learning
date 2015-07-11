package com.aamend.hadoop.MapReduce;

/*
 * To find the Maximum temperature for the years from the weather data set.
 * 
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTemp extends Mapper<LongWritable, Text, Text, IntWritable> {

  int yearIndex = 0;
  int lengthIndex = 3;
  int tempIndex = 1;
  int qualityIndex = 2;

  private static final int Lost = 9999;

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String Line = value.toString();
    String[] recordSplits = Line.split("\t");
    String Year = recordSplits[yearIndex];

    int airTemperature;
    if (recordSplits.length == lengthIndex) {
      airTemperature = Integer.parseInt(recordSplits[tempIndex]);

      String quality = recordSplits[qualityIndex];

      // Checking the record for its quality and irregular data

      if (airTemperature != Lost && quality.matches("[01459]")) {
        context.write(new Text(Year), new IntWritable(airTemperature));
      }
    }

  }
}
