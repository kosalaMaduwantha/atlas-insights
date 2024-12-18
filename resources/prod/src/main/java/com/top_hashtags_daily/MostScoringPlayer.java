package com.top_hashtags_daily;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import com.top_hashtags_daily.BaseBallProcess.ProcessMapper;
import com.top_hashtags_daily.BaseBallProcess.ProcessMapper.ProcessReducer;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MostScoringPlayer {

    private static final Logger logger = Logger.getLogger(MostScoringPlayer.class);

    public static class ProcessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private int player1IDIndex = 6, player1Name = 7, scoreIndex = 24, matchIDIndex = 2, periodIndex = 5;
        private String contentJsonMapper = null;
        private JSONObject jsonObject = null;
        private int previousScoreMargin = 0;
        private boolean isFirstRecord = true;
        private int previousMatchId = 0;
        private BufferedWriter csvWriter;
        private FSDataOutputStream outputStream;

        private static String replaceMonthWithNumber(String score, JSONObject jsonObject) {
            for (String key : jsonObject.keySet()) {
                if (score.contains(key)) {
                    return score.replace(key, jsonObject.get(key).toString());
                }
            }
            return score;
        }

        @Override
        public void setup(Context context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path outputPath = new Path("/user/hadoop/mapper_output/processed.csv");
            outputStream = fs.create(outputPath, true);
            csvWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            csvWriter.write("player_id," + "player_name," + "quarter," + "score");
            csvWriter.newLine();
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
                return;
            }

            if (!fields[player1IDIndex].isEmpty() && !fields[player1Name].isEmpty() && !fields[scoreIndex].isEmpty() && 
                !fields[matchIDIndex].isEmpty()) {

                String playerID = fields[player1IDIndex];
                String playerName = fields[player1Name];
                String score = fields[scoreIndex];
                String matchID = fields[matchIDIndex];
                String period = fields[periodIndex];

                try {
                    String parsedScore = replaceMonthWithNumber(score, jsonObject);
                    int homeScore = Integer.parseInt(parsedScore.split("-")[0]);
                    int visitorScore = Integer.parseInt(parsedScore.split("-")[1]);
                    int scoreMargin = Math.abs(homeScore - visitorScore);
                    int marginMatchID = Integer.parseInt(matchID);

                    if (isFirstRecord) {
                        isFirstRecord = false;
                        previousScoreMargin = scoreMargin;
                        previousMatchId = marginMatchID;
                        logger.warn("Skipping first record: " + line);
                        return;
                    }
                        if (marginMatchID != previousMatchId) {
                        previousScoreMargin = 0;
                        previousMatchId = marginMatchID;
                        logger.warn("Skipping record with different match ID: " + line);
                        return;
                    }

                    int scoreDiff = Math.abs(scoreMargin - previousScoreMargin);

                    csvWriter.write(playerID + "," + playerName + "," + period + "," + scoreDiff);
                    csvWriter.newLine();
                    
                    context.write(new Text(playerID + "-" + playerName), new IntWritable(scoreDiff));
                    previousScoreMargin = scoreMargin;
                } catch (Exception e) {
                    logger.error("Error parsing score: " + score, e);
                }
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException {
            if (csvWriter != null) {
                csvWriter.close();
            }
        }
    }

    public static class ProcessReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<String, Integer> playerScores = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            playerScores.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String topPlayer = null;
            int maxScore = 0;
            for (Map.Entry<String, Integer> entry : playerScores.entrySet()) {
                if (entry.getValue() > maxScore) {
                    maxScore = entry.getValue();
                    topPlayer = entry.getKey();
                }
            }
            if (topPlayer != null) {
                context.write(new Text(topPlayer), new IntWritable(maxScore));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.map.log.level", "INFO");
        conf.set("mapreduce.reduce.log.level", "INFO");

        if (args.length != 2) {
            System.err.println("Usage: MostScoringPlayer <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf, "BasketBall Process");
        job.setJarByClass(MostScoringPlayer.class);
        job.setMapperClass(ProcessMapper.class);
        job.setReducerClass(ProcessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
