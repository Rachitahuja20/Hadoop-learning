/*
MR job for Filter the records for the songs with title containing "night"

O/P : Output record which has only records with title containing keyword.
 */
package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class FilterMapper extends
    Mapper<Object, Text, Text, DoubleWritable> {

  private Logger logger = Logger.getLogger("FilterMapper");

  private final int incomeIndex = 54;
  private final int countryIndex = 0;
  private final int lenIndex = 58;

  String seek = "Adjusted net national income (current US$)";
  String seperator = ",";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      logger.info("null found.");
      return;
    }
    if (line.toString().contains(
        "Adjusted net national income per capita (current US$)")) {
      String[] recordSplits = line.toString().split(seperator);

      logger.info("The data has been splitted.");

      if (recordSplits.length == lenIndex) {

        String countryName = recordSplits[countryIndex];
        try {

          double income = Double.parseDouble(recordSplits[incomeIndex]);

          context.write(new Text(countryName), new DoubleWritable(income));

        } catch (NumberFormatException nfe) {

          logger.info("The value of income is in wrong format.");

          return;
        }

      }
    }
  }
}
