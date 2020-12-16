package com.miracle.dev.workdemo;

import com.miracle.bean.Sensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MySource implements SourceFunction<Sensor> {

    @Override
    public void run(SourceContext<Sensor> ctx) throws Exception {
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            ctx.collect(Sensor.builder().id(String.valueOf(random.nextInt(4))).temperature(100D).timestamp(100L).build());
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {

    }
}