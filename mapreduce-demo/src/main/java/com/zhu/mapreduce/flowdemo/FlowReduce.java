package com.zhu.mapreduce.flowdemo;

import com.zhu.mapreduce.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author ZhuHaiBo
 * @Create 2021/5/18 23:33
 */
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        AtomicLong sumDownFlow = new AtomicLong();
        AtomicLong sumUpFlow = new AtomicLong();

        values.forEach(flowBean -> {
            sumDownFlow.addAndGet(flowBean.getDownFlow());
            sumUpFlow.addAndGet(flowBean.getUpFlow());
        });

        FlowBean flowBean = new FlowBean(sumUpFlow.get(), sumDownFlow.get(), sumDownFlow.get() + sumUpFlow.get());

        context.write(key, flowBean);

    }
}
