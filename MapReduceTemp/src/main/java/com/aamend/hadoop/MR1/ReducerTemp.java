package com.aamend.hadoop.MR1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerTemp extends
    Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {

    Double maxValue = Double.MIN_VALUE;
    for (DoubleWritable value : values) {
      // To find the maximum temperature for the year provided to the reducer
      maxValue = Math.max(maxValue, value.get());

    }
    context.write(key, new DoubleWritable(maxValue));
  }
}
