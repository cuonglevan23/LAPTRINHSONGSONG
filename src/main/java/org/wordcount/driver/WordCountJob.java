package org.wordcount.driver;

import org.wordcount.mapper.TokenizerMapper;
import org.wordcount.reducer.IntSumReducer;
import org.wordcount.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class WordCountJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Properties props = new Properties();

        try (FileInputStream input = new FileInputStream("src/main/resources/config.properties")) {
            props.load(input);
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountJob.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(props.getProperty(Constants.INPUT_PATH)));
        FileOutputFormat.setOutputPath(job, new Path(props.getProperty(Constants.OUTPUT_PATH)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
