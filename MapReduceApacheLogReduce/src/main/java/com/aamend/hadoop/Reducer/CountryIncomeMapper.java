/*
MR job to Filter the records for the country name and its adjusted net national income per capita (current US$) for the year 2010.

O/P : Output record with country name and its adjusted net national income per capita (current US$) for the year 2010. 
Note: The input file should be in foo.*format*.gz format.

 */
package com.aamend.hadoop.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class CountryIncomeMapper extends
    Mapper<Object, Text, Text, IntWritable> {

  private Logger logger = Logger.getLogger("FilterMapper");

  private final int statusIndex = 8;
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

      if (statusCode.length() > 3 || statusCode.length() < 2) {
        context.getCounter(COUNTERS.MISSING_FIELDS_RECORD_COUNT).increment(1);
        return;
      }

      IntWritable ONE = new IntWritable();
      ONE.set(1);
      context.write(new Text(statusCode), ONE);

    } else
      context.getCounter(COUNTERS.MISSING_FIELDS_RECORD_COUNT).increment(1);

  }
}
