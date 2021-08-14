package com.zhu.boot.cdc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.zhu.boot.cdc.entity.OrderItem;
import org.apache.ibatis.annotations.Mapper;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/12 1:54
 */
@Mapper
public interface OrderItemMapper extends BaseMapper<OrderItem> {
}
