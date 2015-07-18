package com.aamend.hadoop.MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.aamend.hadoop.Reducer.CountryIncomeMapper;
import com.aamend.hadoop.Reducer.CountryIncomeReducer;

//import com.aamend.hadoop.MR_Realdata.ReducerTemp1;

public class MRjobTest {
  MapDriver<Object, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

  @Before
  public void setUp() {
    CountryIncomeMapper mapper = new CountryIncomeMapper();
    CountryIncomeReducer reducer = new CountryIncomeReducer();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapDriver = MapDriver.newMapDriver(mapper);
    // mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  // Test for line 1 in the input data
  public void testMapper_normalrec_normalmap() throws IOException {
    mapDriver
        .withInput(
            new LongWritable(),
            new Text(
                "ix-ont5-14.ix.netcom.com - - [01/Jul/1995:01:09:27 -0400] GET /shuttle/countdown/video/livevideo.jpeg HTTP/1.0 404 -"));
    mapDriver.withOutput(new Text("404"), new IntWritable(1));
    mapDriver.runTest();
  }

  @Test
  // Test for another line in the input data
  public void testMapper_normalrec_normal() throws IOException {
    mapDriver
        .withInput(
            new LongWritable(),
            new Text(
                "asp.erinet.com - - [01/Jul/1995:00:07:30 -0400] GET /images/MOSAIC-logosmall.gif HTTP/1.0 200 363"));
    mapDriver.withOutput(new Text("200"), new IntWritable(1));
    mapDriver.runTest();
  }

  @Test
  public void testReducer_validinput_validoutput() throws IOException {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    reduceDriver.withInput(new Text("200"), values);
    reduceDriver.withOutput(new Text("200"), new IntWritable(2));
    reduceDriver.runTest();
  }

  // @Test
  // public void testMapReduce_validinput_validoutput() throws IOException {
  // mapReduceDriver.withInput(new LongWritable(), new Text(
  // "   2014  12    8.7     2.4       9    44.0    96.9 "));
  // List<DoubleWritable> values = new ArrayList<DoubleWritable>();
  // values.add(new DoubleWritable(8.7));
  // values.add(new DoubleWritable(6));
  // mapReduceDriver.withOutput(new Text("2014"), new DoubleWritable(8.7));
  // mapReduceDriver.runTest();
}
// }