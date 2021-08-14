package com.zhu.boot.cdc;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/12 1:36
 */
@SpringBootApplication
@MapperScan("com.zhu.boot.cdc.*")
public class BootExecApplication {
    public static void main(String[] args) {
        SpringApplication.run(BootExecApplication.class, args);
    }
}
