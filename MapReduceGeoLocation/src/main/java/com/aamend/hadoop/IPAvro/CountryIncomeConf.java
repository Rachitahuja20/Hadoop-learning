package com.aamend.hadoop.IPAvro;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CountryIncomeConf {

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    Path inputPath = new Path(args[0]);
    Path inputPath1 = new Path(args[1]);
    Path outputDir = new Path(args[2]);

    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "CountryIncomeConf");
    job.setJarByClass(CountryIncomeConf.class);

    // Decompressing .gz file Ex. foo.csv.gz to foo.csv

    String uri = args[0];
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(inputPath);
    if (codec == null) {
      System.err.println("No codec found for " + uri);
      System.exit(1);
    }
    String outputUri =
        CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
    InputStream in = null;
    OutputStream out = null;
    try {
      in = codec.createInputStream(fs.open(inputPath));
      out = fs.create(new Path(outputUri));
      IOUtils.copyBytes(in, out, conf);
    } finally {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
    }
    DistributedCache.addCacheFile(inputPath1.toUri(), job.getConfiguration());

    // Setup MapReduce
    job.setMapperClass(CountryIncomeMapper.class);
    job.setReducerClass(CountryIncomeReducer.class);
    job.setNumReduceTasks(1);

    // Specify key / value
    job.setOutputValueClass(NullWritable.class);

    // Input
    FileInputFormat.addInputPaths(job, outputUri);
    job.setInputFormatClass(TextInputFormat.class);

    // Output
    FileOutputFormat.setOutputPath(job, outputDir);
    AvroJob
        .setOutputKeySchema(
            job,
            Pair.getPairSchema(Schema.create(Type.STRING),
                Schema.create(Type.INT)));
    job.setOutputValueClass(NullWritable.class);

    job.setInputFormatClass(TextInputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setSortComparatorClass(Text.Comparator.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

    // Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true);

    // Execute job
    int code = job.waitForCompletion(true) ? 0 : 1;

    // Counter finding and displaying
    Counters counters = job.getCounters();

    // Displaying counters
    System.out.printf("Missing Fields: %d, Error Count: %d\n", counters
        .findCounter(COUNTERS.MISSING_FIELDS_RECORD_COUNT).getValue(), counters
        .findCounter(COUNTERS.NULL_OR_EMPTY).getValue());

    System.exit(code);

  }

}