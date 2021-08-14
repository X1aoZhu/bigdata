package com.zhu.boot.cdc.controller;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.zhu.boot.cdc.dto.OrderDto;
import com.zhu.boot.cdc.dto.OrderItemDto;
import com.zhu.boot.cdc.dto.ProductDto;
import com.zhu.boot.cdc.entity.Order;
import com.zhu.boot.cdc.entity.OrderItem;
import com.zhu.boot.cdc.entity.Product;
import com.zhu.boot.cdc.mapper.OrderItemMapper;
import com.zhu.boot.cdc.mapper.OrderMapper;
import com.zhu.boot.cdc.mapper.ProductMapper;
import com.zhu.boot.cdc.service.OrderItemService;
import com.zhu.boot.cdc.util.MyIdUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/12 1:36
 */
@Slf4j
@RestController
public class OrderController {

    @Resource
    private OrderMapper orderMapper;

    @Resource
    private OrderItemMapper orderItemMapper;

    @Resource
    private OrderItemService orderItemService;

    @Resource
    private ProductMapper productMapper;

    @GetMapping("/createOrder")
    @Transactional(rollbackFor = Exception.class)
    public OrderDto createOrder() {
        Long orderId = MyIdUtil.getId();
        //order
        Order order = Order.builder().id(orderId)
                .status(1)
                .createTime(DateTime.now())
                .updateTime(DateTime.now())
                .build();

        //orderItem
        List<Product> productList = productMapper.selectList(new QueryWrapper<>());
        List<Product> productScoreList = new ArrayList<>();
        for (int i = 0; i <= RandomUtil.randomInt(1, productList.size()); i++) {
            Product product = productList.get(RandomUtil.randomInt(0, productList.size()));
            productScoreList.add(product);
        }
        List<OrderItem> orderItemList = new ArrayList<>(productScoreList.size());
        for (Product product : productScoreList) {
            OrderItem orderItem = new OrderItem();
            orderItem.setId(MyIdUtil.getId());
            orderItem.setOrderId(orderId);
            orderItem.setProductId(product.getId());

            int count = RandomUtil.randomInt(0, 20);
            orderItem.setProductCount(count);
            orderItem.setOrderItemAmount(product.getPrice().multiply(new BigDecimal(count)));

            orderItem.setCreateTime(DateTime.now());
            orderItem.setUpdateTime(DateTime.now());
            orderItemList.add(orderItem);
        }

        double totalAmount = orderItemList.stream()
                .mapToDouble(orderItem -> orderItem.getOrderItemAmount().doubleValue()).sum();
        order.setTotalAmount(new BigDecimal(totalAmount));

        orderMapper.insert(order);
        orderItemService.saveBatch(orderItemList);

        // 生成的订单信息封装返回
        OrderDto result = new OrderDto();
        ArrayList<OrderItemDto> orderItemDtoList = new ArrayList<>();
        orderItemList.forEach(orderItem -> {
            OrderItemDto orderItemDto = new OrderItemDto();
            BeanUtils.copyProperties(orderItem, orderItemDto);
            orderItemDtoList.add(orderItemDto);
        });
        result.setOrderItemList(orderItemDtoList);
        return result;
    }

    @GetMapping("/orderInfoList")
    public List<OrderDto> getOrderInfoList(String orderId) {
        List<Order> orderList = new ArrayList<>();
        if (StringUtils.isNotBlank(orderId)) {
            orderList.add(orderMapper.selectById(orderId));
        } else {
            orderList = orderMapper.selectList(new QueryWrapper<>());
        }

        List<Long> orderIdList = Optional.ofNullable(orderList).orElse(CollUtil.newArrayList())
                .stream().map(Order::getId).distinct().collect(Collectors.toList());
        QueryWrapper<OrderItem> itemQueryWrapper = new QueryWrapper<>();
        itemQueryWrapper.in("order_id", orderIdList);
        List<OrderItem> orderItemList = orderItemMapper.selectList(itemQueryWrapper);

        Map<Long, List<OrderItem>> orderItemMap =
                orderItemList.stream().collect(Collectors.groupingBy(OrderItem::getOrderId));

        List<OrderDto> result = new ArrayList<>(orderList.size());
        orderList.forEach(order -> {
            OrderDto orderDto = new OrderDto();
            BeanUtils.copyProperties(order, orderDto);
            List<OrderItem> tempItemList = orderItemMap.get(order.getId());

            ArrayList<OrderItemDto> orderItemDtos = new ArrayList<>();
            tempItemList.forEach(item -> {
                OrderItemDto orderItemDto = new OrderItemDto();
                BeanUtils.copyProperties(item, orderItemDto);
                orderItemDtos.add(orderItemDto);
            });
            orderDto.setOrderItemList(orderItemDtos);
        });
        return result;
    }

    @GetMapping("/updateOrderStatus/{orderId}")
    @Transactional(rollbackFor = Exception.class)
    public OrderDto updateOrderStatus(@PathVariable("orderId") String orderId) {
        Order order = orderMapper.selectById(orderId);
        Assert.notNull(order, "illegal orderId");
        order.setStatus(2);
        order.setUpdateTime(DateTime.now());
        orderMapper.updateById(order);

        QueryWrapper<OrderItem> orderItemQueryWrapper = new QueryWrapper<>();
        orderItemQueryWrapper.eq("order_id", orderId);
        List<OrderItem> orderItems = orderItemMapper.selectList(orderItemQueryWrapper);

        List<OrderItemDto> orderItemDtoList = new ArrayList<>();
        orderItems.forEach(orderItem -> {
            OrderItemDto orderItemDto = new OrderItemDto();
            BeanUtils.copyProperties(orderItem, orderItemDto);
            orderItemDtoList.add(orderItemDto);
        });

        OrderDto orderDto = new OrderDto();
        BeanUtils.copyProperties(order, orderDto);
        orderDto.setOrderItemList(orderItemDtoList);
        return orderDto;
    }

    @PostMapping("/createProduct")
    public Product createProduct(@RequestBody ProductDto productDto) {
        Long id = MyIdUtil.getId();

        Product product = Product.builder().id(id)
                .name(StringUtils.isNotBlank(productDto.getName()) ? productDto.getName() : "product_" + id)
                .price(productDto.getPrice())
                .createTime(DateTime.now())
                .updateTime(DateTime.now())
                .build();

        productMapper.insert(product);
        return product;
    }

    @PostMapping("/updateProduct")
    public Product updateProduct(@RequestBody ProductDto productDto) {
        Product product = productMapper.selectById(productDto.getId());
        Assert.notNull(product, "no such product by id = " + productDto.getId());

        product.setName(productDto.getName());
        product.setPrice(productDto.getPrice());
        product.setUpdateTime(DateTime.now());
        productMapper.updateById(product);

        return product;
    }

}
