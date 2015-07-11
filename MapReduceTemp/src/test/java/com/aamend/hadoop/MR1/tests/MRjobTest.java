package com.aamend.hadoop.MR1.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.aamend.hadoop.MR1.MapperTemp1;
import com.aamend.hadoop.MR1.ReducerTemp1;


public class MRjobTest {
  MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
  ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

  @Before
  public void setUp() {
    MapperTemp1 mapper = new MapperTemp1();
    ReducerTemp1 reducer = new ReducerTemp1();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  // Test for line 1 in the input data
  public void testMapper_normalrec_normalmap() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "   1853   1    8.4     2.7       4    62.8     ---"));
    mapDriver.withOutput(new Text("1853"), new DoubleWritable(8.4));
    mapDriver.runTest();
  }

  @Test
  // Test for last line in the input data
  public void testMapper_normalrec_normal() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "   2014  12    8.7     2.4       9    44.0    96.9 "));
    mapDriver.withOutput(new Text("2014"), new DoubleWritable(8.7));
    mapDriver.runTest();
  }

  @Test
  // Test for malformed data in the input data
  public void testMapper_malformeddata_validmap() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "   2014  12    ?     2.4       9    44.0    96.9 "));
    // mapDriver.withOutput(new Text("2014"), new DoubleWritable(8.7));
    mapDriver.runTest();
  }

  @Test
  public void testReducer_validinput_validoutput() throws IOException {
    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
    values.add(new DoubleWritable(6));
    values.add(new DoubleWritable(8.7));
    reduceDriver.withInput(new Text("2014"), values);
    reduceDriver.withOutput(new Text("2014"), new DoubleWritable(8.7));
    reduceDriver.runTest();
  }

  @Test
  public void testMapReduce_validinput_validoutput() throws IOException {
    mapReduceDriver.withInput(new LongWritable(), new Text(
        "   2014  12    8.7     2.4       9    44.0    96.9 "));
    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
    values.add(new DoubleWritable(8.7));
    values.add(new DoubleWritable(6));
    mapReduceDriver.withOutput(new Text("2014"), new DoubleWritable(8.7));
    mapReduceDriver.runTest();
  }
}