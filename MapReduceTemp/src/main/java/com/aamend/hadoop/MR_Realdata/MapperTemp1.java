package com.aamend.hadoop.MR_Realdata;

/*
 * To find the Maximum temperature for the years from the weather data set.
 * 
 */
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTemp1 extends
    Mapper<LongWritable, Text, Text, DoubleWritable> {

  int yearIndex = 0;
  int expectLen = 7;
  int tempIndex = 2;
  String Seperator = " +";

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String Line = value.toString();

    if (Line.isEmpty() || Line == null) {
      return;
    }

    String[] recordSplits = Line.trim().split(Seperator);

    if (recordSplits.length == expectLen) {

      String Year = recordSplits[yearIndex];

      Double airTemperature;

      try {

        airTemperature = Double.parseDouble(recordSplits[tempIndex]);

        // Checking the record for its quality and irregular data

        context.write(new Text(Year), new DoubleWritable(airTemperature));

      } catch (NumberFormatException nfe) {

        return;
      }
    }
  }
}
