package com.aamend.hadoop.MapReduce;

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

import com.aamend.hadoop.MapReduce.FilterMapper;

//import com.aamend.hadoop.MR_Realdata.ReducerTemp1;

public class MRjobTest {
  MapDriver<Object, Text, Text, DoubleWritable> mapDriver;
  ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

  @Before
  public void setUp() {
    FilterMapper mapper = new FilterMapper();
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
                "Albania,ALB,Adjusted net national income per capita (current US$),NY.ADJ.NNTY.PC.CD,,,,,,,,,,,,,,,,,,,,,,,,,5.12912796410165E+02,5.25364554266154E+02,5.97994870626119E+02,5.76295111326175E+02,5.52138840671851E+02,5.91867263357457E+02,5.23649968404791E+02,2.72696177980108E+02,1.64171133586430E+02,3.31482713027813E+02,5.53133661887050E+02,6.88993957773544E+02,8.65429971281896E+02,6.32939798078532E+02,7.89319529935219E+02,9.91255797833798E+02,1.08034436853913E+03,1.21543794050343E+03,1.31506584578293E+03,1.68174063125608E+03,2.20917586641443E+03,2.47432568466251E+03,2.71200150923595E+03,3.19267040640632E+03,3.77714473402274E+03,3.49810317962131E+03,3.47727494354761E+03,3.75755415923017E+03,3.57535976010380E+03,3.77127022434756E+03,"));
    mapDriver.withOutput(new Text("Albania"), new DoubleWritable(
        3477.27494354761));
    mapDriver.runTest();
  }

  @Test
  // Test for last line in the input data
  public void testMapper_normalrec_normal() throws IOException {
    mapDriver
        .withInput(
            new LongWritable(),
            new Text(
                "Albania,ALB,Adjusted net national income per capita (current US$),NY.ADJ.NNTY.PC.CD,,,,,,,,,,,,,,,,,,,,,,,,,5.12912796410165E+02,5.25364554266154E+02,5.97994870626119E+02,5.76295111326175E+02,5.52138840671851E+02,5.91867263357457E+02,5.23649968404791E+02,2.72696177980108E+02,1.64171133586430E+02,3.31482713027813E+02,5.53133661887050E+02,6.88993957773544E+02,8.65429971281896E+02,6.32939798078532E+02,7.89319529935219E+02,9.91255797833798E+02,1.08034436853913E+03,1.21543794050343E+03,1.31506584578293E+03,1.68174063125608E+03,2.20917586641443E+03,2.47432568466251E+03,2.71200150923595E+03,3.19267040640632E+03,3.77714473402274E+03,3.49810317962131E+03,,3.75755415923017E+03,3.57535976010380E+03,3.77127022434756E+03,"));
    // mapDriver.withOutput(new Text("Albania"), new DoubleWritable());
    mapDriver.runTest();
  }

  // @Test
  // // Test for malformed data in the input data
  // public void testMapper_malformeddata_validmap() throws IOException {
  // mapDriver.withInput(new LongWritable(), new Text(
  // "   2014  12    ?     2.4       9    44.0    96.9 "));
  // // mapDriver.withOutput(new Text("2014"), new DoubleWritable(8.7));
  // mapDriver.runTest();
  // }

  // @Test
  // public void testReducer_validinput_validoutput() throws IOException {
  // List<DoubleWritable> values = new ArrayList<DoubleWritable>();
  // values.add(new DoubleWritable(6));
  // values.add(new DoubleWritable(8.7));
  // reduceDriver.withInput(new Text("2014"), values);
  // reduceDriver.withOutput(new Text("2014"), new DoubleWritable(8.7));
  // reduceDriver.runTest();
  // }

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