package com.aamend.hadoop.MapReduce;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.aamend.hadoop.MapReduce.Mapper1.Map;

public class Conf {

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: AvroWordCount <input path> <output path>");
      return -1;
    }
    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "Conf");
    job.setJarByClass(Conf.class);

    // We call setOutputSchema first so we can override the configuration
    // parameters it sets
    AvroJob
        .setOutputKeySchema(
            job,
            Pair.getPairSchema(Schema.create(Type.STRING),
                Schema.create(Type.INT)));
    job.setOutputValueClass(NullWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setSortComparatorClass(Text.Comparator.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

    return 0;
}

  public static void main(String[] args) throws Exception {
    int res =
        ToolRunner.run(new Configuration(), new Mapper1(), args);
    System.exit(res);
  }
}