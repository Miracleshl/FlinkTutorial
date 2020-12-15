package com.miracle.dev.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Random;

public class WindowTest {
    @Test
    public void fullTimeWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> fileSource = env.addSource(new MySource());
        fileSource.flatMap((String line, Collector<Tuple2<String, Double>> collector) -> {
            String[] arr = line.split(",");
            collector.collect(Tuple2.of(arr[0], Double.valueOf(arr[2])));
        }, Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10L)))
                .process(new MyProcessWindowFunction())
                .print();
        env.execute("Time window");
    }
    @Test
    public void incrementTimeWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> fileSource = env.addSource(new MySource());
        DataStream<Tuple2<String, Double>> sink = fileSource.flatMap((String line, Collector<Tuple2<String, Double>> collector) -> {
            String[] arr = line.split(",");
            collector.collect(Tuple2.of(arr[0], Double.valueOf(arr[2])));
        }, Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10L)))
                .reduce((latest,current)->{
                    latest.setField(Double.min(latest.f1, current.f1),1);
                    return latest;
                });
        sink.print();
        sink.writeAsText("D:\\fileSink\\output1.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        File file = new File("");


        env.execute("Time window");
    }
}

class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<Tuple2<String, Double>> elements, Collector<Tuple2<String, Double>> out) throws Exception {
        Tuple2<String, Double> result = null;
        int i = 0;
        for (Tuple2<String, Double> element : elements) {
            i++;
            if (result == null) result = element;
            result.setField(Double.min(result.f1, element.f1), 1);
        }
        System.out.println(i);
        out.collect(result);
    }
}

class MySource implements SourceFunction<String> {

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Random random = new Random();
        while (true) {
            for (int i = 1; i < 4; i++) {
                String temp = "Sensor_" + i + "," + System.currentTimeMillis() + "," + (random.nextDouble() * 100D + random.nextGaussian());
                System.out.println(temp);
                ctx.collect(temp);
            }
            Thread.sleep(5000L);
        }
    }

    @Override
    public void cancel() {

    }
}
