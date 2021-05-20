package com.zhu.mapreduce.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @Author ZhuHaiBo
 * @Create 2021/5/10 1:17
 */
public class WordCountCombineDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(WordCountCombineDriver.class);

        //job.setInputFormatClass(CombineFileInputFormat.class);
        //CombineFileInputFormat.setMaxInputSplitSize(job,4194304);

        job.setMapperClass(WordCountCombineMapper.class);
        job.setReducerClass(WordCountCombineReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,2048);

        FileInputFormat.setInputPaths(job, new Path("F:\\hadoop\\combine"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\hadoop\\combine\\output3"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 1 : 0);
    }
}
