/*
MR job to Filter the records for the country name and its adjusted net national income per capita (current US$) for the year 2010.

O/P : Output record with country name and its adjusted net national income per capita (current US$) for the year 2010. 
Note: The input file should be in foo.*format*.gz format.

 */
package com.aamend.hadoop.sort;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class ApacheLogMapper extends
    Mapper<Object, Text, Text, IntWritable> {

  private Logger logger = Logger.getLogger("FilterMapper");

  private final int statusIndex = 3;
  private final int lenIndex = 9;

  String seperator = " ";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null | !!line.toString().isEmpty()) {
      logger.info("null found.");
      context.getCounter(COUNTERS.NULL_OR_EMPTY).increment(1);
      return;
    }

    // Splitting the record with a space and removing eveything except
    // [^0-9a-zA-Z ]
    String[] recordSplits =
        line.toString().replaceAll("[^0-9a-zA-Z ]", "").toLowerCase()
            .split(seperator);

    logger.info("The data has been splitted.");

    if (recordSplits.length >= lenIndex) {

      String statusCode = recordSplits[statusIndex];
      // String time = statusCode.substring(0, 9);
      //
      // SimpleDateFormat formatter = new SimpleDateFormat("ddMMMyyyyhhmmss");
      //
      // try {
      //
      // Date date = formatter.parse(statusCode);
      // System.out.println(date);
      //
      // System.out.println(formatter.format(date));
      //
      // } catch (ParseException e) {
      // e.printStackTrace();
      // }

      if (statusCode.length() > 3 || statusCode.length() < 2) {
        context.getCounter(COUNTERS.MISSING_FIELDS_RECORD_COUNT).increment(1);
        return;
      }
      // Checking for status messages and incrementing the counters
      if (statusCode.matches("200")) {
        context.getCounter(COUNTERS.StatusCode200).increment(1);
      } else if (statusCode.matches("302")) {
        context.getCounter(COUNTERS.StatusCode302).increment(1);
      } else if (statusCode.matches("304")) {
        context.getCounter(COUNTERS.StatusCode304).increment(1);
      } else if (statusCode.matches("404")) {
        context.getCounter(COUNTERS.StatusCode404).increment(1);
      } else
        context.getCounter(COUNTERS.MISSING_FIELDS_RECORD_COUNT).increment(1);
    }
  } // No context.write method as mapper function is not ouputting anything.
}
