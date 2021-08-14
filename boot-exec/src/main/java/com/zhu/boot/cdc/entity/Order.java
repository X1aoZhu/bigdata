package com.zhu.boot.cdc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/12 1:48
 */
@Data
@Builder
@TableName("sys_order")
@NoArgsConstructor
@AllArgsConstructor
public class Order implements Serializable {

    private Long id;

    private BigDecimal totalAmount;

    /**
     * 订单状态，1：未支付，2：已支付
     */
    private Integer status;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC")
    private Date updateTime;

}
