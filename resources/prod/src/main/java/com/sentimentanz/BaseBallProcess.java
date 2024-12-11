package com.sentimentanz;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
// import input stream 
import java.io.InputStream;
import org.apache.commons.io.IOUtils;



import org.json.JSONObject;

import com.sentimentanz.BaseBallProcess.ProcessMapper;
import com.sentimentanz.BaseBallProcess.ProcessMapper.ProcessReducer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class BaseBallProcess {
    private static final Logger logger = Logger.getLogger(BaseBallProcess.class);

    public static class ProcessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private int teamIndex = 11, periodIndex = 5, scoreIndex = 24, matchIDIndex = 2;
        private String contentJsonMapper = null;
        private JSONObject jsonObject = null;
        private int previousScoreMargin = 0;
        private int previousMatchId = 0;
        private boolean isFirstRecord = true;

        private static String replaceMonthWithNumber(String score, JSONObject jsonObject) {
            for (String key : jsonObject.keySet()) {
                if (score.contains(key)) {
                    return score.replace(key, jsonObject.get(key).toString());
                }
            }
            return score;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (contentJsonMapper == null || jsonObject == null) {
                try (InputStream inputStream = getClass().getResourceAsStream("/month_to_score.json")) {
                    if (inputStream == null) {
                        throw new FileNotFoundException("Resource not found: /month_to_score.json");
                    }
                    contentJsonMapper = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                    jsonObject = new JSONObject(contentJsonMapper);
                } catch (IOException e) {
                    logger.error("Error reading month_to_score.json file", e);
                }
            }
            String line = value.toString();
            String[] fields = line.split(",");

            
            if (fields.length <= scoreIndex) {
                logger.warn("Skipping line due to insufficient fields: " + line);
                return ;
            }

            if (!fields[teamIndex].isEmpty() && !fields[periodIndex].isEmpty() && !fields[scoreIndex].isEmpty()
                    && !fields[matchIDIndex].isEmpty()) {
                
                String team = fields[teamIndex];
                String period = fields[periodIndex];
                String score = fields[scoreIndex];
                String matchID = fields[matchIDIndex];
                try{
                    String parsedScore = replaceMonthWithNumber(score, jsonObject);
                    int homeScore = Integer.parseInt(parsedScore.split("-")[0]);
                    int visitorScore = Integer.parseInt(parsedScore.split("-")[1]);
                    int scoreMargin = Math.abs(homeScore - visitorScore);
                    int marginMatchID = Integer.parseInt(matchID);
    
                    // check if the record is the first record
                    if (isFirstRecord) {
                        isFirstRecord = false;
                        previousScoreMargin = scoreMargin;
                        previousMatchId = marginMatchID;
                        logger.warn("Skipping first record: " + line);
                        return;
                    }
                    // check if the score margin is greater than the previous score margin
                    if (marginMatchID != previousMatchId) {
                        previousScoreMargin = 0;
                        previousMatchId = marginMatchID;
                        logger.warn("Skipping record with different match ID: " + line);
                        return;
                    }

                    int scoreDiff = Math.abs(scoreMargin - previousScoreMargin);
                    context.write(new Text(team + "-" + period), new IntWritable(scoreDiff));
                    previousScoreMargin = scoreMargin;
                } catch (Exception e) {
                    logger.error("Error parsing score: " + score, e);
                    e.printStackTrace();
                }

            }
        }

        public static class ProcessReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

            private IntWritable result = new IntWritable();

            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
            }
        
            
        }

       
    }

    public static void main(String[] args) throws Exception {
        // create map reduce configuration for the job
        Configuration conf = new Configuration();
        conf.set("mapreduce.map.log.level", "INFO");
        conf.set("mapreduce.reduce.log.level", "INFO");
        if (args.length != 2) {
            System.err.println("Usage: BaseBallProcess <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance(conf, "BaseBall Process");
        job.setJarByClass(BaseBallProcess.class);
        job.setMapperClass(ProcessMapper.class);
        job.setReducerClass(ProcessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set the input and output path for the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // exit the job after completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
