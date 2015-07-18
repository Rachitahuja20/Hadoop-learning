package com.aamend.hadoop.MapReduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class SecondarySortBasicGroupingComparator extends WritableComparator {
  protected SecondarySortBasicGroupingComparator() {
    super(CompositeKeyWritable.class, true);
  }

  private Logger logger = Logger.getLogger("FilterMapper");

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
    CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

    return key1.getIncome().compareTo(key2.getIncome());
  }
}