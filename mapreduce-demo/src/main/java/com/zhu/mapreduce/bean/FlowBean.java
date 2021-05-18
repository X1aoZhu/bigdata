package com.zhu.mapreduce.bean;

import lombok.*;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 上行下行流向
 *
 * @Author ZhuHaiBo
 * @Create 2021/5/18 23:09
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class FlowBean implements Writable, Comparable<FlowBean> {

    /**
     * 上行流量
     */
    private long upFlow;

    /**
     * 下行流量
     */
    private long downFlow;

    /**
     * 总流量
     */
    private long totalFlow;

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
    }

    /**
     * 序列化方法
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(totalFlow);
    }

    /**
     * 反序列化方法，读取顺序要保持和序列化方法写入一致
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        totalFlow = in.readLong();
    }

    @Override
    public int compareTo(FlowBean flowBean) {
        return this.getTotalFlow() > flowBean.getTotalFlow() ? -1 : 1;
    }
}
