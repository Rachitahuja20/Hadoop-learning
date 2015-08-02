package com.aamend.hadoop.MapReduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce
    extends
    Reducer<Text, IntWritable, AvroWrapper<Pair<CharSequence, Integer>>, NullWritable> {

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable value : values) {
      sum += value.get();
    }
    context.write(new AvroWrapper<Pair<CharSequence, Integer>>(
        new Pair<CharSequence, Integer>(key.toString(), sum)), NullWritable
        .get());
}
}