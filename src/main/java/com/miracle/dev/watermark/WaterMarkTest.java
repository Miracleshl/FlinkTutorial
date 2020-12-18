package com.miracle.dev.watermark;

import com.miracle.bean.Sensor;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.time.Duration;

public class WaterMarkTest {
    @Test
    public void oldWaterMark() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Sensor> streamSource = environment.addSource(null);
        OutputTag<Sensor> late = new OutputTag<>("late", TypeInformation.of(Sensor.class));

        SingleOutputStreamOperator<Sensor> reduce = streamSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Sensor>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(Sensor element) {
                return element.getTimestamp() * 1000L;
            }
        })
                .keyBy(Sensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .reduce((latest, current) -> {
                    latest.setTemperature(Double.min(latest.getTemperature(), current.getTemperature()));
                    latest.setTimestamp(current.getTimestamp());
                    return latest;
                });
        reduce.getSideOutput(late).print("late>>>");
        reduce.print("result>>>");
        environment.execute();
    }

    @Test
    public void newWaterMark() throws Exception {
        //注意空闲时间的使用
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.getConfig().setAutoWatermarkInterval(50);
        DataStream<String> socketSource = environment.socketTextStream("192.168.3.9", 7777);
        OutputTag<Sensor> late = new OutputTag<>("late", TypeInformation.of(Sensor.class));
        SingleOutputStreamOperator<Sensor> reduce = socketSource.flatMap((line, collector) -> {
            String[] arr = line.split(",");
            collector.collect(Sensor.builder().id(arr[0]).timestamp(Long.valueOf(arr[1])).temperature(Double.valueOf(arr[2])).build());
        }, Types.POJO(Sensor.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Sensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                //注意空闲时间的使用
                                .withTimestampAssigner(TimestampAssignerSupplier.of((element, recordTimestamp) -> element.getTimestamp() * 1000L)).withIdleness(Duration.ofSeconds(1)))
                .keyBy(Sensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .reduce((latest, current) -> {
                    latest.setTemperature(Double.min(latest.getTemperature(), current.getTemperature()));
                    latest.setTimestamp(current.getTimestamp());
                    return latest;
                });
        reduce.getSideOutput(late).print("late>>>");
        reduce.print("result>>>");
        environment.execute();
    }
}
