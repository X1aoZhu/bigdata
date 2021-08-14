package com.zhu.boot.cdc.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.zhu.boot.cdc.entity.OrderItem;
import com.zhu.boot.cdc.mapper.OrderItemMapper;
import com.zhu.boot.cdc.service.OrderItemService;
import org.springframework.stereotype.Service;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/13 1:49
 */
@Service
public class OrderItemServiceImpl extends ServiceImpl<OrderItemMapper, OrderItem> implements OrderItemService {
}
