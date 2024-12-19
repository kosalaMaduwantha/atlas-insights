package com.top_hashtags_monthly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.top_hashtags_daily.HashTagPopAnz.HashtagCSVMapper;
import com.top_hashtags_daily.HashTagPopAnz.HashtagCSVReducer;

import java.io.IOException;
import java.util.regex.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HashTagPopAnz {
    
    public static class HashtagCSVMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Logger logger = Logger.getLogger(HashtagCSVMapper.class);
        
        private Text outputKey = new Text();
        private int dateIndex = 1, hashIndex = 2, freqIndex = 3;
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(","); 
            if (line.matches(".*id,date,hash_tag,freq.*")) {
                return;
            }
            if (fields.length >= 4) {
                try {
                    String date = fields[dateIndex];
                    String[] dateParts = date.split("-");
                    String yearMonth = dateParts[0] + "-" + dateParts[1];

                    String hashTag = fields[hashIndex];
                    int freqInt = Integer.parseInt(fields[freqIndex].strip());

                    outputKey.set(yearMonth + "\t" + hashTag);
                    context.write(outputKey, new IntWritable(freqInt));
                } catch (Exception e) {
                    String errorMessage = String.format(
                        "Error parsing frequency from line: '%s' Exception: %s", 
                        line, 
                        e.getMessage());
                    throw new IOException(errorMessage, e);
                }
            }
            
        }
    }
    public static class HashtagCSVReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: HashtagCSVAnalysis daily <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance(conf, "Hashtag CSV Popularity Analysis");
        job.setJarByClass(HashTagPopAnz.class);
        job.setMapperClass(HashtagCSVMapper.class);
        job.setReducerClass(HashtagCSVReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
    