package com.sentimentanz;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class SentimentAnzProcessDataTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        SentimentAnzProcessData.HashtagCSVMapper mapper = new SentimentAnzProcessData.HashtagCSVMapper();
        SentimentAnzProcessData.HashtagCSVReducer reducer = new SentimentAnzProcessData.HashtagCSVReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws Exception {
        mapDriver.withInput(new LongWritable(1), new Text("\"0\",\"1467810369\",\"Mon Apr 06 22:19:45 PDT 2009\",\"NO_QUERY\",\"user\",\"Just another #example tweet\""))
                 .withOutput(new Text("2009-04-06\t#example"), new IntWritable(1))
                 .runTest();
    }

    @Test
    public void testReducer() throws Exception {
        reduceDriver.withInput(new Text("2009-04-06\t#example"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
                    .withOutput(new Text("2009-04-06\t#example"), new IntWritable(2))
                    .runTest();
    }
}