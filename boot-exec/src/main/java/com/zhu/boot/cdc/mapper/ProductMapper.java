package com.zhu.boot.cdc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.zhu.boot.cdc.entity.Product;
import org.apache.ibatis.annotations.Mapper;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/12 1:55
 */
@Mapper
public interface ProductMapper extends BaseMapper<Product> {
}
