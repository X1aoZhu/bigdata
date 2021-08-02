package com.zhu.flume.interception;

import com.zhu.flume.utils.JsonUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Flume处理Json拦截器
 *
 * @Author ZhuHaiBo
 * @Create 2021/7/31 16:58
 */
public class LogsInterception implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // json校验
        String jsonStr = new String(event.getBody(), StandardCharsets.UTF_8);
        if (JsonUtils.validJsonStr(jsonStr)) {
            return event;
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        //Iterator<Event> iterator = list.iterator();
        //while (iterator.hasNext()){
        //    Event next = iterator.next();
        //    if(intercept(next)==null){
        //        iterator.remove();
        //    }
        //}
        list.removeIf(next -> intercept(next) == null);
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogsInterception();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
