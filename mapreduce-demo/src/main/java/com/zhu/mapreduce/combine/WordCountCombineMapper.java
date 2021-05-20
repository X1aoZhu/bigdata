package com.zhu.mapreduce.combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author ZhuHaiBo
 * @Create 2021/5/9 17:26
 */
public class WordCountCombineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text text = new Text();

    private IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //map阶段默认按行读取，每一行单词之间以空格为间隔
        String[] wordArray = value.toString().split(" ");
        for (String word : wordArray) {
            text.set(word);
            context.write(text, intWritable);
        }
    }
}
