package com.miracle.dev.source;

import com.miracle.bean.Sensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class SourceTest {
    @Test
    public void defineSource() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceFunction<Sensor> mySource = new MySource();
        DataStream<Sensor> defineSource = environment.addSource(mySource);
        defineSource.print();
        environment.execute("Define Source");
    }

    private class MySource implements SourceFunction<Sensor> {
        private Boolean running = Boolean.TRUE;

        @Override
        public void run(SourceContext<Sensor> ctx) throws Exception {
            Random random = new Random();
            List<Sensor> list = new ArrayList(3);
            list.add(Sensor.builder().id("1").temperature(random.nextDouble() * 100).timestamp(System.currentTimeMillis()).build());
            list.add(Sensor.builder().id("2").temperature(random.nextDouble() * 100).timestamp(System.currentTimeMillis()).build());
            list.add(Sensor.builder().id("3").temperature(random.nextDouble() * 100).timestamp(System.currentTimeMillis()).build());
            while (running) {
                Thread.sleep(5000);
                long timestamp = System.currentTimeMillis();
                list.forEach(sensor -> {
                    sensor.setTemperature(sensor.getTemperature() + random.nextGaussian());
                    sensor.setTimestamp(timestamp);
                    ctx.collect(sensor);
                });
            }
        }

        @Override
        public void cancel() {
            running = Boolean.FALSE;
        }
    }
}
