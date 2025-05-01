package com.preprocess;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.top_hashtags_daily.HashTagPopAnz.HashtagCSVMapper;
import com.top_hashtags_daily.HashTagPopAnz.HashtagCSVReducer;

public class Preprocess {
    
    public static class PreprocessMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
        private static final Logger logger = Logger.getLogger(HashtagCSVMapper.class);
        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();
        private int idIndex = 1, dateIndex = 2, textIndex = 5;
        private BufferedWriter csvWriter;
        private FSDataOutputStream outputStream;

        private static final Pattern URL_PATTERN = Pattern.compile("https?://\\S+");
        private static final Pattern HASHTAG_PATTERN = Pattern.compile("#(\\w+)");
        private static final Pattern SPECIAL_CHAR_PATTERN = Pattern.compile("[^\\w\\s#]");

        private SimpleDateFormat inputDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
        private SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public void setup(Context context) throws IOException {
            try{
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path outputPath = new Path("/user/kosala/mapper_output/processed.csv");
                outputStream = fs.create(outputPath, true);
                csvWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
                csvWriter.write("id," + "date," + "hash_tag," + "freq,");
                csvWriter.newLine();
            } catch (Exception e) {
                logger.error("Error in setup: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\",\""); 
            
            if (fields.length < 6){
                try{
                    System.err.println("Invalid line: " + line);
                    logger.error("Invalid line: " + line);
                    throw new IOException("Invalid line: " + line);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Error in map: " + e.getMessage());
                }
                
            }
            if (fields.length >= 6) {
                try {
                    
                    // clean up the data
                    String id = fields[idIndex];
                    String timestamp = fields[dateIndex].replace("\"", ""); 
                    String text = fields[textIndex].replace("\"", ""); 

                    text = URL_PATTERN.matcher(text).replaceAll(""); 
                    text = SPECIAL_CHAR_PATTERN.matcher(text).replaceAll(""); 

                    System.out.println("text: " + text);

                    String date = "unknown";
                    try {
                        Date parsedDate = inputDateFormat.parse(timestamp);
                        date = outputDateFormat.format(parsedDate);
                    } catch (Exception e) {
                        // If timestamp parsing fails, use "unknown"
                        e.printStackTrace();    
                    }

                    
                    Matcher matcher = HASHTAG_PATTERN.matcher(text);
                    while (matcher.find()) {
                        String hashtag = matcher.group(1).toLowerCase();
                        csvWriter.write(id + "," + date + ",#" + hashtag + "," + one);
                        csvWriter.newLine();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Error in map: " + e.getMessage());
                }
            }
        }
    }

    // public static class PreprocessReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //     private IntWritable result = new IntWritable();

    //     @Override
    //     protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    //         int sum = 0;
    //         for (IntWritable val : values) {
    //             sum += val.get();
    //         }
    //         result.set(sum);
    //         context.write(key, result);
    //     }
    // }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: PreprocessAnz <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance(conf, "Hashtag CSV Popularity Analysis");
        job.setJarByClass(Preprocess.class);
        job.setMapperClass(PreprocessMapper.class);

        // Set the number of reduce tasks to zero for a Map-only job
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
