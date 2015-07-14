/*
MR job for Filter the records for the songs with title containing "night"

O/P : Output record which has only records with title containing keyword.
 */
package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class FilterMapper extends Mapper<Object, Text, ClickKey, NullWritable> {

  private final int incomeIndex = 54;
  private final int indicatorIndex = 2;
  private final int countryIndex = 0;
  private final int lenIndex = 58;

  String seek = "Adjusted net national income (current US$)";
  String seperator = ",";

  private Logger logger = Logger.getLogger("FilterMapper");

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      return;
    }
    if (line.toString().contains(
        "Adjusted net national income per capita (current US$)")) {
      String[] recordSplits = line.toString().split(seperator);

      String v = recordSplits[indicatorIndex];
      logger.info("Stage 0");
      if (recordSplits.length == lenIndex) {

        String countryName = recordSplits[countryIndex];
        try {
          Double income = Double.parseDouble(recordSplits[incomeIndex]);

          logger.info("Stage 1");

          ClickKey pair = new ClickKey();

          pair.setCountryName(countryName);

          pair.setIncome(income);

          logger.info("Stage 2");

          context.write(pair, NullWritable.get());

        } catch (NumberFormatException nfe) {
          logger.info("Stage 3");
          return;
        }

      }
    }
  }
}
