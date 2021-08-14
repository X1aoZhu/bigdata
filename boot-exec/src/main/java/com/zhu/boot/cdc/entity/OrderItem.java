package com.zhu.boot.cdc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/12 1:48
 */
@Data
@TableName("sys_order_item")
@NoArgsConstructor
@AllArgsConstructor
public class OrderItem {

    private Long id;

    private Long orderId;

    private Long productId;

    private Integer productCount;

    private BigDecimal orderItemAmount;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date updateTime;
}
