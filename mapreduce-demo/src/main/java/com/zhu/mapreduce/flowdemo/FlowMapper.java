package com.zhu.mapreduce.flowdemo;

import com.zhu.mapreduce.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author ZhuHaiBo
 * @Create 2021/5/18 23:18
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private FlowBean flowBean = new FlowBean();

    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //格式 "1	13736230513	192.196.100.1	www.bilibili.com	2481	24681	200"
        String[] flowArr = value.toString().split("\t");

        String phoneNumber = flowArr[1];
        long upFlow = Long.parseLong(flowArr[flowArr.length - 3]);
        long downFlow = Long.parseLong(flowArr[flowArr.length - 2]);

        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);

        text.set(phoneNumber);

        context.write(text, flowBean);
    }
}
