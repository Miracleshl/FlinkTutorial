package com.miracle.windowapi;

import com.miracle.bean.Sensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.time.Duration;

public class WindowsApiTest {
    @Test
    public void windowsApiOld() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //以事件时间驱动
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //watermark生成周期间隔
        env.getConfig().setAutoWatermarkInterval(50);
//        String path = "E:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
//        DataStream<String> source = env.readTextFile(path);
        DataStream<String> source = env.socketTextStream("192.168.3.9", 7777);
        DataStream<Sensor> sensorDataStream = source.map(line -> {
            String[] strings = line.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        });

        OutputTag<Sensor> lateTag = new OutputTag<>("late", TypeInformation.of(Sensor.class));
        //每15秒统计一次各传感器平均值
        SingleOutputStreamOperator<Sensor> dataStream = sensorDataStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Sensor>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(Sensor sensor) {
                        return sensor.getTimestamp() * 1000L;
                    }
                }).keyBy(sensor -> sensor.getId())
                //底层通过时间戳处理 但是默认是伦敦时间所以在按照day开窗是要注意
//                .window(TumblingEventTimeWindows.of(Time.seconds(15),Time.seconds(5)))
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(10),Time.seconds(3)))
//                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//                .countWindow(1L)
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .reduce((curRes, newData) -> {
                    curRes.setTemperature(Double.min(curRes.getTemperature(), newData.getTemperature()));
//                    curRes.setTimestamp(Long.max(curRes.getTimestamp(), newData.getTimestamp()));
                    curRes.setTimestamp(newData.getTimestamp());
                    return curRes;
                });
        dataStream.getSideOutput(lateTag).print("side>>>");
        dataStream.print("result>>>");
        env.execute("window test");
    }

    @Test
    public void windowsApiNew() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //以事件时间驱动 时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //watermark生成周期间隔
        env.getConfig().setAutoWatermarkInterval(500);
        String path = "E:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
//        DataStream<String> source = env.readTextFile(path);
        DataStream<String> source = env.socketTextStream("192.168.137.128", 7777);
        DataStream<Sensor> sensorDataStream = source.map(line -> {
            String[] strings = line.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        });
        SerializableTimestampAssigner<Tuple3<String, Double, Long>> assigner = (element, recordTimestamp) -> element.f2 * 1000L;
        WatermarkStrategy<Tuple3<String, Double, Long>> strategy = WatermarkStrategy.<Tuple3<String, Double, Long>>forBoundedOutOfOrderness(Duration.ofMillis(30)).withTimestampAssigner(assigner);
        //每15秒统计一次各传感器平均值
        sensorDataStream.map(sensor -> new Tuple3<>(sensor.getId(), sensor.getTemperature(), sensor.getTimestamp()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.LONG))
                .assignTimestampsAndWatermarks(strategy)
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Double, Long>>forBoundedOutOfOrderness(Duration.ofMillis(30)).withTimestampAssigner((element, recordTimestamp) -> element.f2))
                .keyBy(tuple -> tuple.f0)
                //底层通过时间戳处理 但是默认是伦敦时间所以在按照day开窗是要注意
//                .window(TumblingEventTimeWindows.of(Time.seconds(15),Time.seconds(5)))
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(10),Time.seconds(3)))
//                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//                .countWindow(1L)
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.seconds(1))
                .sideOutputLateData(new OutputTag<>("late"))
                .reduce((curRes, newData) -> {
                    curRes.setField(Double.min(curRes.f1, newData.f1), 1);
                    curRes.setField(Long.max(curRes.f2, newData.f2), 2);
                    return curRes;
                }).getSideOutput(new OutputTag<>("late"))
                .print();
        env.execute("window test");

    }

    @Test
    public void windowsApiCustom() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //以事件时间驱动
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //watermark生成周期间隔
        env.getConfig().setAutoWatermarkInterval(500);
        String path = "E:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
//        DataStream<String> source = env.readTextFile(path);
        DataStream<String> source = env.socketTextStream("192.168.137.128", 7777);
        DataStream<Sensor> sensorDataStream = source.map(line -> {
            String[] strings = line.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        });
        //每15秒统计一次各传感器平均值
        sensorDataStream.map(sensor -> new Tuple3<>(sensor.getId(), sensor.getTemperature(), sensor.getTimestamp()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.LONG))
                .assignTimestampsAndWatermarks((context) -> new WatermarkGenerator<Tuple3<String, Double, Long>>() {
                    /** The maximum timestamp encountered so far. */
                    private long maxTimestamp;

                    /** The maximum out-of-orderness that this watermark generator assumes. */
                    private final long outOfOrdernessMillis = 3000;

                    @Override
                    public void onEvent(Tuple3<String, Double, Long> event, long eventTimestamp, WatermarkOutput output) {
                        maxTimestamp = Long.max(event.f2, maxTimestamp);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis));
                    }
                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Double, Long>>forBoundedOutOfOrderness(Duration.ofMillis(30)).withTimestampAssigner((element, recordTimestamp) -> element.f2))
                .keyBy(tuple -> tuple.f0)
                //底层通过时间戳处理 但是默认是伦敦时间所以在按照day开窗是要注意
//                .window(TumblingEventTimeWindows.of(Time.seconds(15),Time.seconds(5)))
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(10),Time.seconds(3)))
//                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//                .countWindow(1L)
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.seconds(1))
                .sideOutputLateData(new OutputTag<>("late"))
                .reduce((curRes, newData) -> {
                    curRes.setField(Double.min(curRes.f1, newData.f1), 1);
                    curRes.setField(Long.max(curRes.f2, newData.f2), 2);
                    return curRes;
                }).getSideOutput(new OutputTag<>("late"))
                .print();
        env.execute("window test");

    }

    @Test
    public void scaleWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(50);
        DataStream<String> inputStream = env.socketTextStream("192.168.137.128", 7777);
        // 先转换成样例类类型（简单转换操作）
        DataStream<Sensor> dataStream = inputStream.map(line -> {
            String[] strings = line.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Sensor>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(Sensor sensor) {
                return sensor.getTimestamp() * 1000L;
            }
        });

        OutputTag<Tuple3<String, Double, Long>> latetag = new OutputTag("late", Types.TUPLE(Types.STRING, Types.DOUBLE, Types.LONG));
        // 每15秒统计一次，窗口内各传感器所有温度的最小值，以及最新的时间戳
        SingleOutputStreamOperator resultStream = dataStream
                .map(data -> Tuple3.of(data.getId(), data.getTemperature(), data.getTimestamp()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.LONG))
                .keyBy("f0")    // 按照二元组的第一个元素（id）分组
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(latetag)
                .reduce((curRes, newData) -> Tuple3.of(curRes.f0, Double.min(curRes.f1,newData.f1), newData.f2));

        resultStream.getSideOutput(latetag).print("late");
        resultStream.print("result");

        env.execute("window test");
    }
}
