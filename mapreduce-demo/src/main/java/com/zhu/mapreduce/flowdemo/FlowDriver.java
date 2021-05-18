package com.zhu.mapreduce.flowdemo;

import com.zhu.mapreduce.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author ZhuHaiBo
 * @Create 2021/5/18 23:40
 */
public class FlowDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowDriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\hadoop\\flow\\phone_number.txt"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\hadoop\\flow\\output"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 1 : 0);
    }
}
