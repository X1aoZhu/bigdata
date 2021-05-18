package com.zhu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author ZhuHaiBo
 * @Create 2021/5/9 17:27
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable intWritable = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        intWritable.set(sum);

        context.write(key, intWritable);
    }
}
