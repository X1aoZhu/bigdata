package com.zhu.boot.cdc.dto;

import com.zhu.boot.cdc.entity.OrderItem;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/12 1:59
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderDto implements Serializable {

    private Long orderId;

    private Integer orderStatus;

    private BigDecimal totalAmount;

    private List<OrderItemDto> orderItemList;

}
