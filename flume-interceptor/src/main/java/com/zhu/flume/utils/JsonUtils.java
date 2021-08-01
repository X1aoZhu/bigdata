package com.zhu.flume.utils;

import org.mortbay.util.ajax.JSON;

/**
 * @Author ZhuHaiBo
 * @Create 2021/7/31 16:59
 */
public class JsonUtils {

    /**
     * 校验Json
     *
     * @param jsonStr JsonStr
     * @return
     */
    public static Boolean validJsonStr(String jsonStr) {
        try {
            JSON.parse(jsonStr);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
