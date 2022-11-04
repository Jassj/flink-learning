package com.jie.flink.modules.service.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 自定义水位线生成器
 *
 * @author jie2.yuan
 * @version 1.0.0
 * @since 2022/11/2
 */
public class MyTimeWatermarks<T> implements WatermarkGenerator<T> {

    private static final long MAX_OUT_OF_ORDERNESS = 5000;

    /**
     * 每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者也可以基于事件数据本身去生成 watermark。
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {

    }

    /**
     * 周期性的调用生成新的 watermark: 由配置的watermark生成周期决定。
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(System.currentTimeMillis() - MAX_OUT_OF_ORDERNESS));
    }

}
