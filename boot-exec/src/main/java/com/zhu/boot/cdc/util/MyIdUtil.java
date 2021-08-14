package com.zhu.boot.cdc.util;

import cn.hutool.core.util.IdUtil;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/12 1:57
 */
public class MyIdUtil {

    public static Long getId() {
        return IdUtil.getSnowflake().nextId();
    }


}
