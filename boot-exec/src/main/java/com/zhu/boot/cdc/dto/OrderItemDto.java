package com.zhu.boot.cdc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/13 1:58
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderItemDto implements Serializable {

    private Long orderItemId;

    private Long productId;

    private BigDecimal orderItemAmount;

    private BigDecimal productPrice;

    private Integer productCount;

    private String productName;

}
