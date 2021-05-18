package com.zhu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * key：value：输入原始数据
 * key：map输出，一个单词；value：数量
 * @Author ZhuHaiBo
 * @Create 2021/5/9 17:26
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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
