package org.wordcount.mapper;

import org.wordcount.utils.TextCleaner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        // Không cần đọc stopwords.txt nữa
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        for (String token : tokens) {
            String cleaned = TextCleaner.clean(token);
            if (!cleaned.isEmpty()) { // Không kiểm tra stopwords nữa
                word.set(cleaned);
                context.write(word, one);
            }
        }
    }
}
