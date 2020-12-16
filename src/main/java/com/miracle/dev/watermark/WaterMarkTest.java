package com.miracle.dev.watermark;

import com.miracle.bean.Sensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

public class WaterMarkTest {
    @Test
    public void oldWaterMark() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Sensor> streamSource = environment.addSource(null);
        streamSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Sensor>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(Sensor element) {
                return element.getTimestamp();
            }
        })
                .keyBy(Sensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(null)
                .max("temperature")
                .print();
        environment.execute();
    }

    @Test
    public void newWaterMark() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Sensor> streamSource = environment.addSource(null);
        environment.execute();
    }
}
