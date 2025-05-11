package org.wordcount;

import org.wordcount.mapper.TokenizerMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

public class WordCountTest {
    @Test
    public void testMapper() throws IOException {
        TokenizerMapper mapper = new TokenizerMapper();
        new MapDriver<Object, Text, Text, IntWritable>()
                .withMapper(mapper)
                .withInput(new Text("1"), new Text("The quick brown fox"))
                .withOutput(new Text("the"), new IntWritable(1))    // thêm dòng này
                .withOutput(new Text("quick"), new IntWritable(1))
                .withOutput(new Text("brown"), new IntWritable(1))
                .withOutput(new Text("fox"), new IntWritable(1))
                .runTest();
    }

}
