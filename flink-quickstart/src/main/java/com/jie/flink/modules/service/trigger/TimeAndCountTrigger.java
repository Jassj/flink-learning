package com.jie.flink.modules.service.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.HashMap;
import java.util.Map;


public class TimeAndCountTrigger extends Trigger<String, TimeWindow> {

    private Map<String, Integer> keyCount = new HashMap<>();

    @Override
    public TriggerResult onElement(String value, long valueTimestamp, TimeWindow window, TriggerContext ctx) throws Exception {
		ctx.registerProcessingTimeTimer(window.maxTimestamp());
		int count = keyCount.getOrDefault(value, 0);
        keyCount.put(value, ++count);
        if (count >= 15) {
            keyCount.put(value, 0);
			return TriggerResult.FIRE_AND_PURGE;
		}
		System.out.println(String.format("value[%s], timestamp[%s]", value, ctx.getCurrentProcessingTime()));
		return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }

    public static TimeAndCountTrigger create() {
        return new TimeAndCountTrigger();
    }

}
