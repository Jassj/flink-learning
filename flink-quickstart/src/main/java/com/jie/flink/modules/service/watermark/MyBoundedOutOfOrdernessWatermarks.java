package com.jie.flink.modules.service.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class MyBoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

    /**
     * The maximum timestamp encountered so far.
     */
    private long maxTimestamp;

    /**
     * The maximum out-of-orderness that this watermark generator assumes.
     */
    private final long outOfOrdernessMillis;

    /**
     * Creates a new watermark generator with the given out-of-orderness bound.
     *
     * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
     */
    public MyBoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    // ------------------------------------------------------------------------

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        System.out.println("extract watermark --> " + maxTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        System.out.println("emit watermark --> " + (maxTimestamp - outOfOrdernessMillis));
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }

}
