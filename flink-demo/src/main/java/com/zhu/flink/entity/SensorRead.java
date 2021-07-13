package com.zhu.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author ZhuHaiBo
 * @Create 2021/7/11 10:13
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorRead {

    private String id;

    private Long timestamp;

    private Double temperature;

}
