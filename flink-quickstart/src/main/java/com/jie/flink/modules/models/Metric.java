package com.jie.flink.modules.models;

import lombok.Data;

import java.util.Map;

/**
 * @author yuanjie 2020/05/17 23:24
 */
@Data
public class Metric {
    public String name;
    public long timestamp;
    public Map<String, Object> fields;
}
