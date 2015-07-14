package com.aamend.hadoop.MapReduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public abstract class ClickKeyPartitioner extends Partitioner<ClickKey, Text> {

  @Override
  public int getPartition(ClickKey key, Text value, int numReduceTasks) {

    int hash = 7;

    hash =
        13
            * hash
            + (key.getCountryName() != null ? key.getCountryName().hashCode()
                : 0);

    hash =
        13 * hash + (key.getIncome() != null ? key.getIncome().hashCode() : 0);

    return hash % numReduceTasks;

  }

}