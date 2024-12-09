package com.sentimentanz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sentimentanz.HashTagPopAnz.HashtagCSVMapper;
import com.sentimentanz.HashTagPopAnz.HashtagCSVReducer;

import java.io.IOException;
import java.util.regex.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HashTagPopAnz {

    public static class HashtagCSVMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();

        private static final Pattern URL_PATTERN = Pattern.compile("https?://\\S+");
        private static final Pattern HASHTAG_PATTERN = Pattern.compile("#(\\w+)");
        private static final Pattern SPECIAL_CHAR_PATTERN = Pattern.compile("[^\\w\\s#]");

        private SimpleDateFormat inputDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
        private SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\",\""); 
            
            if (fields.length >= 6) {
                try {
                    
                    // clean up the data
                    String timestamp = fields[2].replace("\"", ""); 
                    String text = fields[5].replace("\"", ""); 

                    text = URL_PATTERN.matcher(text).replaceAll(""); 
                    text = SPECIAL_CHAR_PATTERN.matcher(text).replaceAll(""); 

                    String date = "unknown";
                    try {
                        Date parsedDate = inputDateFormat.parse(timestamp);
                        date = outputDateFormat.format(parsedDate);
                    } catch (Exception e) {
                        // If timestamp parsing fails, use "unknown"
                    }

                    
                    Matcher matcher = HASHTAG_PATTERN.matcher(text);
                    while (matcher.find()) {
                        String hashtag = matcher.group(1).toLowerCase();
                        outputKey.set(date + "\t#" + hashtag);
                        context.write(outputKey, one);
                    }
                } catch (Exception e) {
                    
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
            System.err.println("Usage: HashtagCSVAnalysis <input path> <output path>");
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
